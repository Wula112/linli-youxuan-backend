package com.hmdp.job;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.mq.producer.VoucherOrderPublisher;
import com.hmdp.repository.redis.SeckillPendingRepository;
import com.hmdp.repository.redis.SeckillReservation;
import com.hmdp.job.config.SeckillReconcileProperties;
import com.hmdp.job.config.JobProperties;
import com.hmdp.job.support.JobRun;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.Set;

import static com.hmdp.common.constant.RedisConstants.LOCK_VOUCHER_ORDER_KEY;

// 原职责（保留）：只核对订单、清理已完成索引或补发原消息，不恢复库存、不删除购买资格。
/** 核对订单并有限补发；补发耗尽后与消费者互斥，确认未建单才释放预占。 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "hmdp.seckill.reconcile", name = "enabled", havingValue = "true", matchIfMissing = true)
public class SeckillOrderReconcileJob {

    // pending 扫描、领取与补发计数都通过仓储访问，Lua、Key 和原子范围留在 repository.redis。
    private final SeckillPendingRepository pendingRepository;
    private final VoucherOrderMapper voucherOrderMapper;
    private final VoucherOrderPublisher voucherOrderPublisher;
    private final SeckillReconcileProperties properties;
    private final RedissonClient redissonClient;
    private final JobProperties jobProperties;
    private final SeckillReservation seckillReservation;

    // 原本地入口（调度注解现移入SeckillReconcileConfig，确保与XXL互斥）：
    // @Scheduled(fixedDelayString = "${hmdp.seckill.reconcile.interval:60000}", scheduler = "seckillReconcileScheduler")
    public void reconcile() {
        log.info("秒杀对账批次完成：{}", reconcileBatch());
    }

    @XxlJob("seckillOrderReconcile")
    public void reconcileByXxl() { reconcileBatch().reportToXxl(); }

    public JobRun reconcileBatch() {
        JobRun run = new JobRun(jobProperties.getMaxRunTime());
        if (!run.canContinue()) return run;
        long deadline = System.currentTimeMillis() - properties.getPendingTimeout().toMillis();
        Set<String> members;
        try {
            members = pendingRepository.findDue(deadline, properties.getBatchSize());
        } catch (Exception ex) {
            log.error("扫描秒杀待对账订单失败", ex);
            XxlJobHelper.log(ex);
            run.failed();
            return run;
        }
        if (members == null || members.isEmpty()) {
            return run;
        }
        for (String member : members) {
            if (!run.canContinue()) break;
            try {
                // 原子领取并延期：多实例看到同一批候选时，只有一个实例处理。
                if (pendingRepository.claim(member, deadline)) {
                    run.checked();
                    reconcileOne(member, run);
                } else {
                    run.skipped();
                }
            } catch (Exception ex) {
                if (ex.getCause() instanceof Error error) {
                    throw error;
                }
                run.failed();
                XxlJobHelper.log("对账失败，member={}，原因={}", member, ex.toString());
                if (Thread.currentThread().isInterrupted() || ex.getCause() instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    log.error("秒杀订单对账被中断，保留pending，member={}", member, ex);
                    run.canContinue();
                    return run;
                }
                // 领取时已推迟下一次核查，异常不会让坏记录一直占据当前批次。
                log.error("秒杀订单对账失败，保留pending待下次核查，member={}", member, ex);
            }
        }
        return run;
    }

    private void reconcileOne(String member, JobRun run) throws Exception {
        VoucherOrderMessage message = VoucherOrderMessage.fromPendingMember(member);
        if (clearPendingIfOrderExists(message, run)) {
            return;
        }
        Object reservedOrderId = seckillReservation.getReservedOrderId(message.voucherId(), message.userId());
        if (reservedOrderId == null) {
            log.error("预占Hash记录缺失，不补发、不释放库存，member={}", member);
            XxlJobHelper.log("预占Hash记录缺失，member={}", member);
            run.failed();
            return;
        }
        if (!message.orderId().toString().equals(reservedOrderId)) {
            // 当前用户已经由另一笔预占占用；只移除旧member，不能影响新请求。
            // 原清理：stringRedisTemplate.opsForZSet().remove(SECKILL_PENDING_KEY, member);
            seckillReservation.clearPending(member);
            log.warn("清理已失效的旧订单pending，member={}", member);
            return;
        }
        // 原判断（保留）：fromPendingMember每轮重建消息，retryCount会归零，无法累计。
        // if(message.retryCount() >= properties.getMaxRepublishRetries()){
        //     log.error("秒杀订单补发重试次数达到上限，保留pending待人工核查，member={}", member);
        //     return;
        // }
        // voucherOrderPublisher.publish(message);
        long retryCount = pendingRepository.bumpRetry(member, message.voucherId(), message.userId(),
                message.orderId(), properties.getMaxRepublishRetries());
        if (retryCount < 0) {
            // 原逻辑（保留）：达到上限仅保留pending，等待人工核查。
            // log.error("秒杀订单补发重试次数达到上限，保留pending待人工核查，member={}", member);
            // 0仍表示只核查；最后一轮补发本身不释放，下一次超时领取后才走这里。
            if (properties.getMaxRepublishRetries() > 0) {
                releaseExhaustedReservation(message, run);
            }
            return;
        }
        if (retryCount == 0) {
            return;
        }
        voucherOrderPublisher.publish(new VoucherOrderMessage(message.orderId(), message.userId(),
                message.voucherId(), Math.toIntExact(retryCount)));
        // 确认发出不代表落库，不清pending；由消费提交回调或下次对账确认成功。
        log.warn("秒杀订单尚未落库，已按原订单号补发，member={}，补发轮次={}", member, retryCount);
    }

    private void releaseExhaustedReservation(VoucherOrderMessage message, JobRun run) {
        String member = message.pendingMember();
        RLock lock = redissonClient.getLock(LOCK_VOUCHER_ORDER_KEY + message.voucherId() + ":" + message.userId());
        if (!lock.tryLock()) {
            log.warn("秒杀订单仍在处理，保留预占待下次核查，member={}", member);
            run.skipped();
            return;
        }
        try {
            // 消费者持同一把锁直到事务提交；这里不启事务，重新查询已提交订单，不能复用锁外结果。
            if (clearPendingIfOrderExists(message, run)) {
                return;
            }
            Object reservedOrderId = seckillReservation.getReservedOrderId(message.voucherId(), message.userId());
            if (reservedOrderId == null) {
                log.error("锁内预占Hash记录缺失，不释放库存，保留pending待核查，member={}", member);
                XxlJobHelper.log("锁内预占Hash记录缺失，member={}", member);
                run.failed();
                return;
            }
            if (!message.orderId().toString().equals(reservedOrderId)) {
                seckillReservation.clearPending(member);
                log.warn("锁内预占已变更，只清理旧订单pending，member={}", member);
                return;
            }
            // Lua再次校验归属，原子返还库存并清理资格、pending及计数；迟到消息将因资格不匹配跳过建单。
            Long released = seckillReservation.rollback(message.voucherId(), message.userId(), message.orderId());
            if (released == null || released < 0) {
                throw new IllegalStateException("补发耗尽后的预占释放失败，保留pending待重试，member=" + member);
            }
            log.warn("秒杀订单补发耗尽，已结束原预占，member={}，释放结果={}", member, released);
        } finally {
            lock.unlock();
        }
    }

    /** 订单存在即停止补发/释放；标识冲突时保留pending，匹配时才清理。 */
    private boolean clearPendingIfOrderExists(VoucherOrderMessage message, JobRun run) {
        VoucherOrder order = voucherOrderMapper.selectRoutedOrder(
                message.orderId(), message.userId(), message.voucherId());
        if (order == null) {
            return false;
        }
        if (!Objects.equals(order.getId(), message.orderId())
                || !Objects.equals(order.getUserId(), message.userId())
                || !Objects.equals(order.getVoucherId(), message.voucherId())) {
            log.error("订单标识不匹配，保留pending等待人工核查，member={}", message.pendingMember());
            XxlJobHelper.log("订单标识不匹配，member={}", message.pendingMember());
            run.failed();
            return true;
        }
        // 原清理：stringRedisTemplate.opsForZSet().remove(SECKILL_PENDING_KEY, member);
        seckillReservation.clearPending(message.pendingMember());
        return true;
    }
}
