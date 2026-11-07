package com.hmdp.job;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.entity.VoucherOrderProcess;
import com.hmdp.job.config.JobProperties;
import com.hmdp.job.config.SeckillReconcileProperties;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mapper.VoucherOrderProcessMapper;
import com.hmdp.repository.redis.SeckillPendingRepository;
import com.hmdp.repository.redis.SeckillReservation;
import com.hmdp.service.impl.VoucherOrderCompensationService;
import com.hmdp.service.impl.VoucherOrderCreateService;
import com.hmdp.service.impl.SeckillCircuitBreaker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SeckillOrderReconcileJobTest {
    private final SeckillPendingRepository pending = mock(SeckillPendingRepository.class);
    private final VoucherOrderMapper orders = mock(VoucherOrderMapper.class);
    private final VoucherOrderProcessMapper processes = mock(VoucherOrderProcessMapper.class);
    private final VoucherOrderCreateService creator = mock(VoucherOrderCreateService.class);
    private final VoucherOrderCompensationService compensation = mock(VoucherOrderCompensationService.class);
    private final SeckillCircuitBreaker circuit = mock(SeckillCircuitBreaker.class);
    private final SeckillReservation reservation = mock(SeckillReservation.class);
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final HashOperations<String, Object, Object> cursors = mock(HashOperations.class);
    private final RedissonClient redisson = mock(RedissonClient.class);
    private final RLock lock = mock(RLock.class);
    private final SeckillOrderReconcileJob job = new SeckillOrderReconcileJob(
            pending, orders, processes, creator, compensation, circuit, reservation,
            new SeckillReconcileProperties(), new JobProperties(), redis, redisson);

    @BeforeEach
    void setUp() {
        when(redisson.getLock(SeckillOrderReconcileJob.SCAN_LOCK)).thenReturn(lock);
        when(lock.tryLock()).thenReturn(true);
        when(redis.opsForHash()).thenReturn(cursors);
        when(pending.findDue(anyLong(), anyInt())).thenReturn(Set.of());
        when(processes.findByStatusAfter(anyString(), anyLong(), anyInt())).thenReturn(List.of());
    }

    @Test
    void shouldCleanPendingForCommittedOrder() {
        when(pending.findDue(anyLong(), anyInt())).thenReturn(Set.of("10:5:1000"));
        when(pending.claim(eq("10:5:1000"), anyLong())).thenReturn(true);
        when(orders.selectRoutedOrder(1000L, 5L, 10L))
                .thenReturn(new VoucherOrder().setId(1000L).setUserId(5L).setVoucherId(10L));
        assertTrue(job.reconcileBatch().successful());
        verify(reservation).clearPending("10:5:1000");
        verifyNoInteractions(creator, compensation);
    }

    @Test
    void shouldCreateOrderForUnconfirmedPendingWithoutCompensating() {
        when(pending.findDue(anyLong(), anyInt())).thenReturn(Set.of("10:5:1000"));
        when(pending.claim(eq("10:5:1000"), anyLong())).thenReturn(true);
        assertTrue(job.reconcileBatch().successful());
        verify(creator).createVoucherOrder(argThat(o -> o.getId().equals(1000L)
                && o.getUserId().equals(5L) && o.getVoucherId().equals(10L)));
        verifyNoInteractions(compensation, reservation);
    }

    @Test
    void shouldProcessDeadLettersAndPreviouslyStartedCompensations() {
        var failed = process(1000, "DEAD_LETTER");
        var restoring = process(1001, "COMPENSATING");
        when(processes.findByStatusAfter("DEAD_LETTER", 0, 100)).thenReturn(List.of(failed));
        when(processes.findByStatusAfter("COMPENSATING", 0, 100)).thenReturn(List.of(restoring));
        when(orders.selectRoutedOrder(1001L, 5L, 10L)).thenReturn(new VoucherOrder().setStatus(4));
        assertTrue(job.reconcileBatch().successful());
        verify(creator).createVoucherOrder(argThat(o -> o.getId().equals(1000L)));
        verify(compensation).compensate(1001L);
    }

    @Test
    void shouldReportFailureAndContinueOtherCompensations() {
        when(processes.findByStatusAfter("COMPENSATING", 0, 100))
                .thenReturn(List.of(process(1000, "COMPENSATING"), process(1001, "COMPENSATING")));
        when(orders.selectRoutedOrder(anyLong(), eq(5L), eq(10L))).thenReturn(new VoucherOrder().setStatus(4));
        doThrow(new IllegalStateException("Redis unavailable")).when(compensation).compensate(1000L);
        assertFalse(job.reconcileBatch().successful());
        verify(compensation).compensate(1001L);
        verify(lock).unlock();
    }

    @Test
    void shouldNotCleanMismatchedOrder() {
        when(pending.findDue(anyLong(), anyInt())).thenReturn(Set.of("10:5:1000"));
        when(pending.claim(eq("10:5:1000"), anyLong())).thenReturn(true);
        when(orders.selectRoutedOrder(1000L, 5L, 10L))
                .thenReturn(new VoucherOrder().setId(1000L).setUserId(6L).setVoucherId(10L));
        assertFalse(job.reconcileBatch().successful());
        verifyNoInteractions(reservation);
    }

    @Test
    void shouldSkipWhenAnotherReconciliationOwnsJobLock() {
        when(lock.tryLock()).thenReturn(false);
        assertTrue(job.reconcileBatch().successful());
        verifyNoInteractions(pending, orders, processes);
    }

    @Test
    void openCircuitReportsFailureButStillRecoversAcceptedOrders() {
        when(circuit.isOpen()).thenReturn(true);
        when(circuit.incident()).thenReturn("mq-123");
        when(pending.findDue(anyLong(), anyInt())).thenReturn(Set.of("10:5:1000"));
        when(pending.claim(eq("10:5:1000"), anyLong())).thenReturn(true);
        assertFalse(job.reconcileBatch().successful());
        verify(creator).createVoucherOrder(argThat(o -> o.getId().equals(1000L)));
    }

    @Test
    void legacyUncreatedCompensationReturnsToCreationPath() {
        when(processes.findByStatusAfter("COMPENSATING", 0, 100))
                .thenReturn(List.of(process(1000, "COMPENSATING")));
        assertTrue(job.reconcileBatch().successful());
        verify(creator).createVoucherOrder(argThat(o -> o.getId().equals(1000L)));
        verifyNoInteractions(compensation);
    }

    private VoucherOrderProcess process(long id, String status) {
        return new VoucherOrderProcess().setOrderId(id).setUserId(5L).setVoucherId(10L).setStatus(status);
    }
}
