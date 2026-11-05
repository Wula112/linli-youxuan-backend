package com.hmdp.job;

import com.hmdp.common.enums.OrderProcessStatus;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.entity.VoucherOrderProcess;
import com.hmdp.job.config.JobProperties;
import com.hmdp.job.config.SeckillReconcileProperties;
import com.hmdp.job.support.JobRun;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mapper.VoucherOrderProcessMapper;
import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.repository.redis.SeckillPendingRepository;
import com.hmdp.repository.redis.SeckillReservation;
import com.hmdp.service.impl.VoucherOrderCompensationService;
import com.hmdp.service.impl.VoucherOrderProcessService;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/** 核查 Redis 预占并处理死信和关单留下的补偿，不再补发下单消息。 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "hmdp.seckill.reconcile", name = "enabled", havingValue = "true", matchIfMissing = true)
public class SeckillOrderReconcileJob {
    static final String CURSOR_KEY = "job:seckill-reconcile:cursor";
    static final String SCAN_LOCK = "lock:job:seckill-reconcile";
    private static final int MAX_BATCHES = 10;
    private final SeckillPendingRepository pendingRepository;
    private final VoucherOrderMapper orders;
    private final VoucherOrderProcessMapper processes;
    private final VoucherOrderProcessService processService;
    private final VoucherOrderCompensationService compensationService;
    private final SeckillReservation reservation;
    private final SeckillReconcileProperties properties;
    private final JobProperties jobProperties;
    private final StringRedisTemplate redis;
    private final RedissonClient redisson;

    public void reconcile() { log.info("秒杀对账批次完成：{}", reconcileBatch()); }

    @XxlJob("seckillOrderReconcile")
    public void reconcileByXxl() { reconcileBatch().reportToXxl(); }

    public JobRun reconcileBatch() {
        JobRun run = new JobRun(jobProperties.getMaxRunTime());
        if (!run.canContinue()) return run;
        RLock lock = null;
        boolean acquired = false;
        try {
            // 任务锁保护游标；即使锁失效，业务状态和 Redis 凭证仍保证重复执行安全。
            lock = redisson.getLock(SCAN_LOCK);
            acquired = lock.tryLock();
            if (!acquired) {
                run.skipped();
                return run;
            }
            long phaseNanos = Math.max(1, jobProperties.getMaxRunTime().toNanos() / 3);
            scanPending(run, System.nanoTime() + phaseNanos);
            scanProcesses(OrderProcessStatus.DEAD_LETTER, run, System.nanoTime() + phaseNanos);
            scanProcesses(OrderProcessStatus.COMPENSATING, run, System.nanoTime() + phaseNanos);
        } catch (Exception ex) {
            fail("秒杀对账批次失败", ex, run);
        } finally {
            if (acquired) lock.unlock();
        }
        return run;
    }

    private void scanPending(JobRun run, long deadlineNanos) {
        if (!run.canContinue()) return;
        Set<String> members = pendingRepository.findDue(
                System.currentTimeMillis() - properties.getPendingTimeout().toMillis(), properties.getBatchSize());
        if (members == null) return;
        long cutoff = System.currentTimeMillis() - properties.getPendingTimeout().toMillis();
        for (String member : members) {
            if (!withinBudget(run, deadlineNanos)) return;
            try {
                if (!pendingRepository.claim(member, cutoff)) {
                    run.skipped();
                    continue;
                }
                run.checked();
                VoucherOrderMessage event = VoucherOrderMessage.fromPendingMember(member);
                VoucherOrder order = orders.selectRoutedOrder(event.orderId(), event.userId(), event.voucherId());
                if (order == null) {
                    // 没有死信记录不代表消息丢失；保留预占，等待 RocketMQ 重试或死信登记。
                    log.warn("订单尚未建单，保留预占等待消费或死信，orderId={}", event.orderId());
                    continue;
                }
                if (!Objects.equals(order.getId(), event.orderId())
                        || !Objects.equals(order.getUserId(), event.userId())
                        || !Objects.equals(order.getVoucherId(), event.voucherId())) {
                    throw new IllegalStateException("订单标识不匹配，member=" + member);
                }
                reservation.clearPending(member);
            } catch (Exception ex) {
                fail("核查预占失败，member=" + member, ex, run);
            }
        }
    }

    private void scanProcesses(OrderProcessStatus status, JobRun run, long deadlineNanos) {
        if (!run.canContinue()) return;
        String field = status.name();
        try {
            Object saved = redis.opsForHash().get(CURSOR_KEY, field);
            long cursor = saved == null ? 0 : Long.parseLong(saved.toString());
            for (int batch = 0; batch < MAX_BATCHES && withinBudget(run, deadlineNanos); batch++) {
                List<VoucherOrderProcess> rows = processes.findByStatusAfter(field, cursor, properties.getBatchSize());
                if (rows == null || rows.isEmpty()) {
                    saveCursor(field, 0);
                    return;
                }
                for (VoucherOrderProcess row : rows) {
                    if (!withinBudget(run, deadlineNanos)) {
                        saveCursor(field, cursor);
                        return;
                    }
                    run.checked();
                    try {
                        if (status == OrderProcessStatus.DEAD_LETTER) {
                            processService.requestCompensation(row.getOrderId(), row.getUserId(), row.getVoucherId());
                        } else {
                            compensationService.compensate(row.getOrderId());
                        }
                    } catch (Exception ex) {
                        fail("处理订单补偿失败，orderId=" + row.getOrderId(), ex, run);
                    }
                    cursor = row.getOrderId();
                }
                if (rows.size() < properties.getBatchSize()) {
                    saveCursor(field, 0);
                    return;
                }
                saveCursor(field, cursor);
            }
        } catch (Exception ex) {
            fail("扫描订单处理状态失败，status=" + field, ex, run);
        }
    }

    private boolean withinBudget(JobRun run, long deadlineNanos) {
        if (!run.canContinue()) return false;
        if (System.nanoTime() >= deadlineNanos) {
            run.limited();
            return false;
        }
        return true;
    }

    private void saveCursor(String field, long cursor) {
        redis.opsForHash().put(CURSOR_KEY, field, Long.toString(cursor));
    }

    private void fail(String message, Exception ex, JobRun run) {
        log.error(message, ex);
        XxlJobHelper.log("{}，原因={}", message, ex.toString());
        run.failed();
        if (ex instanceof InterruptedException || ex.getCause() instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
    }
}
