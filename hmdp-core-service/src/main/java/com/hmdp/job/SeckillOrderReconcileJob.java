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
import com.hmdp.service.impl.VoucherOrderCreateService;
import com.hmdp.service.impl.SeckillCircuitBreaker;
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

/** 核查受理请求、恢复建单并处理关单后的 Redis 返库。 */
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
    private final VoucherOrderCreateService createService;
    private final VoucherOrderCompensationService compensationService;
    private final SeckillCircuitBreaker circuitBreaker;
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
            // 熔断标记保持到人工恢复；每轮向 Admin 上报失败，触发配置好的告警。
            if (circuitBreaker.isOpen()) {
                String incident = circuitBreaker.incident();
                log.error("秒杀订单消费重试耗尽，熔断中，首个死信msgId={}", incident);
                XxlJobHelper.log("秒杀订单消费重试耗尽，熔断中，首个死信msgId={}", incident);
                run.failed();
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
                    // 不再等待 MQ；与消费者、订单查询使用同一事务入口并按处理记录仲裁。
                    createService.createVoucherOrder(toOrder(event));
                    continue;
                }
                if (!Objects.equals(order.getId(), event.orderId())
                        || !Objects.equals(order.getUserId(), event.userId())
                        || !Objects.equals(order.getVoucherId(), event.voucherId())) {
                    throw new IllegalStateException("订单标识不匹配，member=" + member);
                }
                reservation.clearPending(member);
            } catch (Exception ex) {
                fail("核查或恢复预占失败，member=" + member, ex, run);
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
                            createService.createVoucherOrder(toOrder(row));
                        } else {
                            VoucherOrder order = orders.selectRoutedOrder(row.getOrderId(), row.getUserId(), row.getVoucherId());
                            if (order == null) {
                                // 兼容升级前未执行 Redis 返库的历史补偿决策。
                                createService.createVoucherOrder(toOrder(row));
                            } else {
                                compensationService.compensate(row.getOrderId());
                            }
                        }
                    } catch (Exception ex) {
                        fail("恢复订单处理失败，orderId=" + row.getOrderId(), ex, run);
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

    private VoucherOrder toOrder(VoucherOrderMessage event) {
        return new VoucherOrder().setId(event.orderId()).setUserId(event.userId()).setVoucherId(event.voucherId());
    }

    private VoucherOrder toOrder(VoucherOrderProcess row) {
        return new VoucherOrder().setId(row.getOrderId()).setUserId(row.getUserId()).setVoucherId(row.getVoucherId());
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
