package com.hmdp.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.common.enums.VoucherOrderStatus;
import com.hmdp.config.VoucherOrderProperties;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.entity.VoucherOrderRouter;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mapper.VoucherOrderRouterMapper;
import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.mq.producer.OrderTimeoutPublisher;
import com.hmdp.repository.redis.SeckillReservation;
import com.hmdp.service.ISeckillVoucherService;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/**
 * 秒杀订单建单：数据库库存扣减、订单落库、分片路由补齐与提交后动作。
 *
 * <p>建单事务由本类方法上的 {@code @Transactional} 开启，必须经 Spring 代理调用：
 * MQ 消费者在持有订单锁时调用，锁范围覆盖整个数据库事务；门面只做委托，
 * 避免同类自调用绕过代理导致事务注解失效。
 */
@Slf4j
@Service
public class VoucherOrderCreateService {

    @Resource
    private VoucherOrderMapper voucherOrderMapper;

    @Resource
    private VoucherOrderRouterMapper voucherOrderRouterMapper;

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private SeckillReservation seckillReservation;

    @Resource
    private VoucherOrderProperties orderProperties;

    @Resource
    private OrderTimeoutPublisher orderTimeoutPublisher;

    @Transactional(rollbackFor = Exception.class)
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 幂等性，同一订单重复投递直接成功，ACK 丢失后重投也不会再次扣减库存。
        VoucherOrder existing = findExistingOrder(voucherOrder.getId());
        if (existing != null) {
            if (!existing.getUserId().equals(voucherOrder.getUserId())
                    || !existing.getVoucherId().equals(voucherOrder.getVoucherId())) {
                throw new IllegalArgumentException("订单标识与已有订单不一致");
            }
            clearPendingAfterCommit(voucherOrder);
            return;
        }
        Object reservedOrderId = seckillReservation.getReservedOrderId(
                voucherOrder.getVoucherId(), voucherOrder.getUserId());
        if (!voucherOrder.getId().toString().equals(reservedOrderId)) {
            // 发送失败的回退已完成，（此时reservedOrderId应为Null）
            // 或发送失败的回退已完成，并且用户已经发起新请求，生成了新订单Id（reservedOrderId），此时旧消息带了旧订单Id。
            log.info("忽略已回退的秒杀订单，orderId={}", voucherOrder.getId());
            return;
        }
        // 原校验（保留）：历史已关闭订单也阻止再次购买。
        // long count = query().eq("user_id", voucherOrder.getUserId())
        //         .eq("voucher_id", voucherOrder.getVoucherId()).count();
        long count = voucherOrderMapper.selectCount(new QueryWrapper<VoucherOrder>()
                .eq("user_id", voucherOrder.getUserId())
                .eq("voucher_id", voucherOrder.getVoucherId())
                .ne("status", VoucherOrderStatus.CLOSED.getCode()));
        if (count > 0) {
            clearPendingAfterCommit(voucherOrder);
            return;
        }
        boolean success = seckillVoucherService.update().setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0).update();
        if (!success) {
            throw new IllegalStateException("数据库秒杀库存不足，voucherId=" + voucherOrder.getVoucherId());
        }
        // 只在首次建单时初始化；前面的幂等分支不会重置状态或延长支付时间。
        LocalDateTime createTime = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
        voucherOrder.setStatus(VoucherOrderStatus.UNPAID.getCode())
                .setCreateTime(createTime)
                .setExpireTime(createTime.plus(orderProperties.getPayTimeout()))
                .setPayTime(null);
        if (voucherOrderMapper.insert(voucherOrder) < 1) {
            throw new IllegalStateException("秒杀订单保存失败，orderId=" + voucherOrder.getId());
        }
        // 与库存、订单处于同一个 XA 事务；只有订单号的入口通过目录补齐分片键。
        if (voucherOrderRouterMapper.insert(new VoucherOrderRouter().setOrderId(voucherOrder.getId())
                .setUserId(voucherOrder.getUserId()).setVoucherId(voucherOrder.getVoucherId())) != 1) {
            throw new IllegalStateException("订单路由保存失败，orderId=" + voucherOrder.getId());
        }
        // 券列表缓存只保存元数据，库存由查询时从 seckill:stock:{voucherId} 读取，
        // 所以建单不再发布缓存失效消息；详见 docs/cache-consistency.md。
        clearPendingAfterCommit(voucherOrder);
        orderTimeoutPublisher.publishAfterCommit(voucherOrder.getId());
    }

    /**
     * 订单提交后清理 pending 索引；建单与门面回退路径共用。
     *
     * <p>事务内只登记回调，提交后才执行；没有活动事务时立即执行。
     */
    public void clearPendingAfterCommit(VoucherOrder order) {
        String member = new VoucherOrderMessage(order.getId(), order.getUserId(), order.getVoucherId()).pendingMember();
        Runnable cleanup = () -> {
            try {
                // 资格Hash始终保留。清理失败由定时对账根据已提交订单再次清理。
                // 原清理：stringRedisTemplate.opsForZSet().remove(SECKILL_PENDING_KEY, member);
                seckillReservation.clearPending(member);
            } catch (Exception ex) {
                log.error("订单已提交但pending清理失败，等待对账重试，member={}", member, ex);
            }
        };
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    cleanup.run();
                }
            });
        } else {
            cleanup.run();
        }
    }

    /** 与门面 getById 相同：先读订单路由目录，再按分片键读取订单表。 */
    private VoucherOrder findExistingOrder(Long orderId) {
        VoucherOrderRouter route = voucherOrderRouterMapper.selectById(orderId);
        return route == null ? null : voucherOrderMapper.selectRoutedOrder(
                route.getOrderId(), route.getUserId(), route.getVoucherId());
    }
}
