package com.hmdp.service.impl;

import com.hmdp.common.enums.VoucherOrderStatus;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.entity.VoucherOrderRouter;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mapper.VoucherOrderRouterMapper;
import com.hmdp.repository.redis.SeckillReservation;
import com.hmdp.service.ISeckillVoucherService;
import jakarta.annotation.Resource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 超时关单：先提交数据库事务（条件关单 + 返还数据库库存），再执行 Redis 补偿。
 *
 * <p>两段刻意分开：不能让 Redis 返库发生在 MySQL 提交之前，也不能把整个方法包进一个
 * {@code @Transactional}，否则补偿失败会连同已确认的关单一起回滚。Redis 补偿与标记
 * 写入各自幂等，失败后由对账/关单扫描重试。
 */
@Service
public class VoucherOrderCloseService {

    @Resource
    private VoucherOrderRouterMapper voucherOrderRouterMapper;

    @Resource
    private VoucherOrderMapper voucherOrderMapper;

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private SeckillReservation seckillReservation;

    @Resource
    private PlatformTransactionManager transactionManager;

    public void closeExpiredOrder(Long orderId) {
        if (orderId == null || orderId <= 0) {
            throw new IllegalArgumentException("订单号不正确");
        }
        VoucherOrderRouter route = voucherOrderRouterMapper.selectById(orderId);
        if (route == null) return;
        // 显式分开数据库事务和Redis补偿，确保不会在MySQL提交前释放购买资格。
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        VoucherOrder closed = transaction.execute(status -> {
            // 条件更新，status == 4 的订单才能关闭，幂等
            int updated = voucherOrderMapper.closeIfExpired(orderId, route.getUserId(), route.getVoucherId());
            VoucherOrder order = voucherOrderMapper.selectRoutedOrder(orderId, route.getUserId(), route.getVoucherId());
            if (updated == 1) {
                // 确认关闭订单，释放库存
                if (!seckillVoucherService.update().setSql("stock = stock + 1")
                        .eq("voucher_id", order.getVoucherId()).update()) {
                    throw new IllegalStateException("关单返还数据库库存失败，orderId=" + orderId);
                }
                // 库存展示读取 seckill:stock:*，Redis 返库成功后自动恢复，无需失效券列表缓存。
            }
            // 重复消息也可以继续完成此前中断的Redis补偿；已支付、未到期或不存在则跳过。
            if (order == null || !Integer.valueOf(VoucherOrderStatus.CLOSED.getCode()).equals(order.getStatus())
                    || Boolean.TRUE.equals(order.getRedisStockRestored())) {
                return null;
            }
            return order;
        });
        if (closed == null) {
            return;
        }
        // 复用原归属校验Lua：只有Hash仍属于本订单才返库并释放资格，重复和迟到补偿均幂等。
        // 考虑这种情况：
        // 订单 O1 超时关闭，执行 Lua，成功释放了用户 A 对券 V 的购买资格。
        // 用户 A 马上又下了新订单 O2，Redis 里重新写入了 O2 的购买资格。
        // 此时 O1 的补偿消息因为“标记写入失败”被重试。
        // 如果 Lua 不校验归属，直接删除用户 A 的资格，就会把 O2 的资格误删掉。
        // 但 Lua 会校验：当前 Redis 里的 member 是不是 O1 的 member？
        // 发现是 O2 的 member，不是 O1，于是不动。
        Long restored = seckillReservation.rollback(closed.getVoucherId(), closed.getUserId(), closed.getId());
        if (restored == null || restored < 0) {
            throw new IllegalStateException("关单返还Redis库存失败，等待重试，orderId=" + orderId);
        }
        // Lua成功但标记写入失败时，下一次重试补偿不会重复返库，也不会清理后续新订单的资格。
        transaction.executeWithoutResult(status -> voucherOrderMapper.markRedisStockRestored(
                orderId, route.getUserId(), route.getVoucherId()));
    }
}
