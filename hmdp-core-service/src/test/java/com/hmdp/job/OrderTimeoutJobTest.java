package com.hmdp.job;

import com.hmdp.config.VoucherOrderProperties;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.job.config.JobProperties;
import com.xxl.job.core.context.XxlJobContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.HashOperations;

import java.util.List;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

class OrderTimeoutJobTest {

    private final VoucherOrderMapper orders = mock(VoucherOrderMapper.class);
    private final IVoucherOrderService service = mock(IVoucherOrderService.class);
    private final VoucherOrderProperties properties = new VoucherOrderProperties();
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final HashOperations<String, Object, Object> cursors = mock(HashOperations.class);
    private final RedissonClient redisson = mock(RedissonClient.class);
    private final RLock lock = mock(RLock.class);
    private final Map<Object, Object> state = new HashMap<>();
    private final OrderTimeoutJob job = new OrderTimeoutJob(orders, service, properties, redis, redisson, new JobProperties());

    @BeforeEach
    void setup() {
        when(redis.opsForHash()).thenReturn(cursors);
        when(redisson.getLock(OrderTimeoutJob.SCAN_LOCK)).thenReturn(lock);
        when(lock.tryLock()).thenReturn(true);
        when(cursors.get(eq(OrderTimeoutJob.CURSOR_KEY), any())).thenAnswer(i -> state.get(i.getArgument(1)));
        doAnswer(i -> { state.put(i.getArgument(1), i.getArgument(2)); return null; })
                .when(cursors).put(eq(OrderTimeoutJob.CURSOR_KEY), any(), any());
    }

    @AfterEach
    void cleanup() { XxlJobContext.setXxlJobContext(null); Thread.interrupted(); }

    @Test
    void shouldCloseExpiredOrdersAndRetryIncompleteRedisRestoreIndependently() {
        when(orders.findExpiredUnpaidOrders(0, 100)).thenReturn(List.of(1000L, 1001L));
        when(orders.findClosedOrdersAwaitingStockRestore(0, 100)).thenReturn(List.of(900L));
        doThrow(new IllegalStateException("Redis unavailable")).when(service).closeExpiredOrder(1000L);
        job.closeExpiredOrders();
        verify(service).closeExpiredOrder(1001L);
        verify(service).closeExpiredOrder(900L);
    }

    @Test
    void shouldContinueOtherScanIfExpiredOrderQueryFails() {
        when(orders.findExpiredUnpaidOrders(0, 100)).thenThrow(new IllegalStateException("query failed"));
        when(orders.findClosedOrdersAwaitingStockRestore(0, 100)).thenReturn(List.of(900L));
        job.closeExpiredOrders();
        verify(service).closeExpiredOrder(900L);
    }

    @Test
    void shouldRetainCursorAcrossRunsSoBadRecordsDoNotBlockLaterOrders() {
        properties.setTimeoutScanBatchSize(1);
        when(orders.findExpiredUnpaidOrders(anyLong(), eq(1)))
                .thenAnswer(invocation -> List.of((long) invocation.getArgument(0) + 1));
        doThrow(new IllegalStateException("bad order")).when(service).closeExpiredOrder(anyLong());
        job.closeExpiredOrders();
        job.closeExpiredOrders();
        verify(service).closeExpiredOrder(1L);
        verify(service).closeExpiredOrder(20L);
        verify(orders).findExpiredUnpaidOrders(10, 1);
    }

    @Test
    void shouldResumeCursorOnDifferentExecutorAndReportFailures() {
        properties.setTimeoutScanBatchSize(1);
        when(orders.findExpiredUnpaidOrders(anyLong(), eq(1)))
                .thenAnswer(i -> List.of((long) i.getArgument(0) + 1));
        job.closeBatch();
        assertEquals("10", state.get("unpaid"));
        new OrderTimeoutJob(orders, service, properties, redis, redisson, new JobProperties()).closeBatch();
        verify(service).closeExpiredOrder(20L);
        assertEquals("20", state.get("unpaid"));
    }

    @Test
    void shouldReportPartialFailureToXxlWithoutStoppingOtherOrders() {
        var context = new XxlJobContext(1, "", 1, 0, null, 0, 1);
        XxlJobContext.setXxlJobContext(context);
        when(orders.findExpiredUnpaidOrders(0, 100)).thenReturn(List.of(1L, 2L));
        doThrow(new IllegalStateException("failed")).when(service).closeExpiredOrder(1L);
        job.closeByXxl();
        assertEquals(500, context.getHandleCode());
        verify(service).closeExpiredOrder(2L);
        verify(lock).unlock();
    }

    @Test
    void shouldSkipBusyBatchWithoutMovingCursor() {
        when(lock.tryLock()).thenReturn(false);
        var result = job.closeBatch();
        assertTrue(result.successful());
        assertEquals(1, result.getSkipped());
        verifyNoInteractions(cursors, orders, service);
        verify(lock, never()).unlock();
    }

    @Test
    void shouldFailWhenCursorCannotBeSaved() {
        doThrow(new IllegalStateException("Redis unavailable"))
                .when(cursors).put(eq(OrderTimeoutJob.CURSOR_KEY), any(), any());
        assertFalse(job.closeBatch().successful());
        verify(lock).unlock();
    }
}
