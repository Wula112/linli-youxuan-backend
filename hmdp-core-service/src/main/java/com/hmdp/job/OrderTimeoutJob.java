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

/** 数据库扫描兜底，与Redis预占补发对账分开；多实例依靠状态条件更新和Lua幂等协作。 */
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
            // 任务锁只保护两个扫描游标；业务关单仍依靠原SQL条件更新和返库Lua幂等。
            lock = redisson.getLock(SCAN_LOCK);
            acquired = lock.tryLock();
            if (!acquired) {
                run.skipped();
                return run;
            }
            scan("unpaid", afterId -> voucherOrderMapper.findExpiredUnpaidOrders(afterId,
                    properties.getTimeoutScanBatchSize()), run);
            if (run.canContinue()) {
                scan("restore", afterId -> voucherOrderMapper.findClosedOrdersAwaitingStockRestore(afterId,
                        properties.getTimeoutScanBatchSize()), run);
            }
        } catch (Exception ex) {
            recordFailure("关单补偿批次失败", ex, run);
        } finally {
            if (acquired) lock.unlock();
        }
        return run;
    }

    private void scan(String field, LongFunction<List<Long>> query, JobRun run) {
        // 两类扫描各用至多一半预算，未支付订单积压时也给返库补偿留出机会。
        long phaseDeadline = System.nanoTime() + jobProperties.getMaxRunTime().toNanos() / 2;
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

    // 以下保留原本地定时扫描实现；原调度注解现移到OrderTimeoutConfig。
    //     // 单线程调度；每轮有上限且保留游标，异常记录不会长期挡住后面的订单。
    //     private long unpaidCursor;
    //     private long restoreCursor;
    //
    //     @Scheduled(fixedDelayString = "${hmdp.order.timeout-scan-interval:60000}", scheduler = "orderTimeoutScheduler")
    //     public void closeExpiredOrders() {
    //         unpaidCursor = scan(afterId -> voucherOrderMapper.findExpiredUnpaidOrders(afterId,
    //                 properties.getTimeoutScanBatchSize()), unpaidCursor);
    //         if (!Thread.currentThread().isInterrupted()) {
    //             restoreCursor = scan(afterId -> voucherOrderMapper.findClosedOrdersAwaitingStockRestore(afterId,
    //                     properties.getTimeoutScanBatchSize()), restoreCursor);
    //         }
    //     }
    //
    //     private long scan(LongFunction<List<Long>> query, long cursor) {
    //         for (int batch = 0; batch < MAX_BATCHES; batch++) {
    //             List<Long> orderIds;
    //             try {
    //                 orderIds = query.apply(cursor);
    //             } catch (Exception ex) {
    //                 log.error("扫描超时订单或待返库订单失败，cursor={}", cursor, ex);
    //                 return cursor;
    //             }
    //             if (orderIds == null || orderIds.isEmpty()) return 0;
    //             for (Long orderId : orderIds) {
    //                 try {
    //                     voucherOrderService.closeExpiredOrder(orderId);
    //                 } catch (Exception ex) {
    //                     log.error("关单或返库失败，保留数据库记录供重试，orderId={}", orderId, ex);
    //                 }
    //                 if (Thread.currentThread().isInterrupted()) return cursor;
    //             }
    //             cursor = orderIds.getLast();
    //             if (orderIds.size() < properties.getTimeoutScanBatchSize()) return 0;
    //         }
    //         return cursor;
    //     }
}
