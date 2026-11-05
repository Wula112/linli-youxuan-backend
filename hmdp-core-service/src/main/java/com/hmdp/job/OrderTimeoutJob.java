package com.hmdp.job;

import com.hmdp.config.VoucherOrderProperties;
import com.hmdp.job.config.JobProperties;
import com.hmdp.job.support.JobRun;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.IVoucherOrderService;
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
import java.util.function.LongFunction;

/** 扫描到期订单；关闭与返还数据库库存共用数据库事务，Redis 由对账任务补偿。 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "hmdp.order", name = "timeout-scan-enabled", havingValue = "true", matchIfMissing = true)
public class OrderTimeoutJob {
    static final String CURSOR_KEY = "job:order-timeout:cursor";
    static final String SCAN_LOCK = "lock:job:order-timeout";
    private static final int MAX_BATCHES = 10;
    private final VoucherOrderMapper voucherOrderMapper;
    private final IVoucherOrderService voucherOrderService;
    private final VoucherOrderProperties properties;
    private final StringRedisTemplate redis;
    private final RedissonClient redisson;
    private final JobProperties jobProperties;

    public void closeExpiredOrders() { log.info("关单补偿批次完成：{}", closeBatch()); }

    @XxlJob("orderTimeoutCompensate")
    public void closeByXxl() { closeBatch().reportToXxl(); }

    public JobRun closeBatch() {
        JobRun run = new JobRun(jobProperties.getMaxRunTime());
        if (!run.canContinue()) return run;
        RLock lock = null;
        boolean acquired = false;
        try {
            // 任务锁只保护扫描游标；业务关单由处理记录与条件更新仲裁。
            lock = redisson.getLock(SCAN_LOCK);
            acquired = lock.tryLock();
            if (!acquired) {
                run.skipped();
                return run;
            }
            scan("unpaid", afterId -> voucherOrderMapper.findExpiredUnpaidOrders(afterId,
                    properties.getTimeoutScanBatchSize()), run);
        } catch (Exception ex) {
            recordFailure("关单补偿批次失败", ex, run);
        } finally {
            if (acquired) lock.unlock();
        }
        return run;
    }

    private void scan(String field, LongFunction<List<Long>> query, JobRun run) {
        long phaseDeadline = System.nanoTime() + jobProperties.getMaxRunTime().toNanos();
        try {
            Object saved = redis.opsForHash().get(CURSOR_KEY, field);
            long cursor = saved == null ? 0 : Long.parseLong(saved.toString());
            for (int batch = 0; batch < MAX_BATCHES && run.canContinue(); batch++) {
                List<Long> ids = query.apply(cursor);
                if (ids == null || ids.isEmpty()) {
                    saveCursor(field, 0);
                    return;
                }
                for (Long id : ids) {
                    if (!run.canContinue() || System.nanoTime() >= phaseDeadline) {
                        run.limited();
                        saveCursor(field, cursor);
                        return;
                    }
                    run.checked();
                    try {
                        voucherOrderService.closeExpiredOrder(id);
                    } catch (Exception ex) {
                        recordFailure("关单或返库失败，orderId=" + id, ex, run);
                    }
                    // 失败也推进游标，避免坏记录堵住后续订单；到末尾归零后会再次核查它。
                    cursor = id;
                }
                if (ids.size() < properties.getTimeoutScanBatchSize()) {
                    saveCursor(field, 0);
                    return;
                }
                saveCursor(field, cursor);
            }
        } catch (Exception ex) {
            recordFailure("扫描或保存关单游标失败，field=" + field, ex, run);
        }
    }

    private void saveCursor(String field, long cursor) {
        redis.opsForHash().put(CURSOR_KEY, field, Long.toString(cursor));
    }

    private void recordFailure(String message, Exception ex, JobRun run) {
        log.error(message, ex);
        XxlJobHelper.log("{}，原因={}", message, ex.toString());
        run.failed();
        if (ex instanceof InterruptedException || ex.getCause() instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        run.canContinue();
    }

}
