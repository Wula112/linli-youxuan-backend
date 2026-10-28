package com.hmdp.job;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.mq.producer.VoucherOrderPublisher;
import com.hmdp.job.config.SeckillReconcileProperties;
import com.hmdp.job.config.JobProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.core.retry.RetryException;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.hmdp.common.constant.RedisConstants.SECKILL_PENDING_KEY;
import static com.hmdp.common.constant.RedisConstants.SECKILL_PENDING_RETRIES_KEY;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SeckillOrderReconcileJobTest {

    @Test
    void shouldReportDatabaseFailureToXxlAndKeepPending() {
        var context = new com.xxl.job.core.context.XxlJobContext(1, "", 1, 0, null, 0, 1);
        com.xxl.job.core.context.XxlJobContext.setXxlJobContext(context);
        try {
            when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenThrow(new IllegalStateException("DB down"));
            job.reconcileByXxl();
            assertEquals(500, context.getHandleCode());
            verifyNoInteractions(publisher);
            verify(redis, never()).execute(any(RedisScript.class), eq(CLEAR_KEYS), anyString());
        } finally {
            com.xxl.job.core.context.XxlJobContext.setXxlJobContext(null);
        }
    }

    @Test
    void shouldTreatBusyOrderLockAsDeferredAndStopBeforeClaimWhenInterrupted() {
        when(redis.execute(any(RedisScript.class), eq(RETRY_KEYS), eq(MEMBER), anyString(), eq("5"), eq("1000")))
                .thenReturn(-1L);
        assertTrue(job.reconcileBatch().successful());
        clearInvocations(redis);
        Thread.currentThread().interrupt();
        try {
            assertFalse(job.reconcileBatch().successful());
            verifyNoInteractions(redis);
        } finally {
            Thread.interrupted();
        }
    }

    private static final String MEMBER = "10:5:1000";
    private static final List<String> RETRY_KEYS = List.of(SECKILL_PENDING_KEY,
            SECKILL_PENDING_RETRIES_KEY, "seckill:reservation:10");
    private static final List<String> CLEAR_KEYS = List.of(SECKILL_PENDING_KEY, SECKILL_PENDING_RETRIES_KEY);
    private static final List<String> ROLLBACK_KEYS = List.of("seckill:stock:10", "seckill:reservation:10",
            SECKILL_PENDING_KEY, SECKILL_PENDING_RETRIES_KEY);
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final ZSetOperations<String, String> pending = mock(ZSetOperations.class);
    private final HashOperations<String, Object, Object> reservations = mock(HashOperations.class);
    private final VoucherOrderMapper orders = mock(VoucherOrderMapper.class);
    private final VoucherOrderPublisher publisher = mock(VoucherOrderPublisher.class);
    private final SeckillReconcileProperties properties = new SeckillReconcileProperties();
    private final RedissonClient redisson = mock(RedissonClient.class);
    private final RLock orderLock = mock(RLock.class);
    private final SeckillOrderReconcileJob job = new SeckillOrderReconcileJob(redis, orders, publisher, properties, redisson, new JobProperties());

    @BeforeEach
    void setUp() {
        when(redis.opsForZSet()).thenReturn(pending);
        when(redis.opsForHash()).thenReturn(reservations);
        when(redisson.getLock("lock:order:10:5")).thenReturn(orderLock);
        when(pending.rangeByScore(eq(SECKILL_PENDING_KEY), eq(0d), anyDouble(), eq(0L), eq(100L)))
                .thenReturn(Set.of(MEMBER));
        when(redis.execute(any(RedisScript.class), eq(List.of(SECKILL_PENDING_KEY)),
                anyString(), anyString(), anyString())).thenReturn(1L);
        when(reservations.get("seckill:reservation:10", "5")).thenReturn("1000");
        when(redis.execute(any(RedisScript.class), eq(RETRY_KEYS), eq(MEMBER), anyString(), eq("5"), eq("1000")))
                .thenReturn(1L);
    }

    @Test
    void shouldClearCommittedOrderWithoutTouchingQualificationOrPublishing() {
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenReturn(new VoucherOrder().setId(1000L).setUserId(5L).setVoucherId(10L));
        job.reconcile();
        verify(redis).execute(any(RedisScript.class), eq(CLEAR_KEYS), eq(MEMBER));
        verifyNoInteractions(reservations, publisher);
    }

    @Test
    void shouldRepublishSameIdentifiersAndLeavePendingUntilDatabaseCommit() throws Exception {
        job.reconcile();
        var sequence = inOrder(redis, orders, reservations, publisher);
        sequence.verify(redis).execute(any(RedisScript.class), eq(List.of(SECKILL_PENDING_KEY)),
                eq(MEMBER), anyString(), anyString());
        sequence.verify(orders).selectRoutedOrder(1000L, 5L, 10L);
        sequence.verify(reservations).get("seckill:reservation:10", "5");
        sequence.verify(redis).execute(any(RedisScript.class), eq(RETRY_KEYS),
                eq(MEMBER), eq("5"), eq("5"), eq("1000"));
        sequence.verify(publisher).publish(new VoucherOrderMessage(1000L, 5L, 10L, 1));
        verify(pending, never()).remove(anyString(), any());
        verify(redis, never()).opsForValue();
    }

    @Test
    void shouldKeepPendingWhenPublishingFails() throws Exception {
        doThrow(new RetryException("MQ unavailable", new IllegalStateException())).when(publisher).publish(any());
        assertDoesNotThrow(job::reconcile);
        verify(pending, never()).remove(anyString(), any());
        verify(reservations, never()).delete(anyString(), any());
    }

    @Test
    void shouldSkipCandidateAlreadyClaimedOrClearedByAnotherInstance() {
        when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString())).thenReturn(0L);
        job.reconcile();
        verifyNoInteractions(orders, reservations, publisher);
    }

    @Test
    void shouldRetainMissingReservationForManualInvestigation() {
        when(reservations.get("seckill:reservation:10", "5")).thenReturn(null);
        job.reconcile();
        verifyNoInteractions(publisher);
        verify(pending, never()).remove(anyString(), any());
    }

    @Test
    void shouldRemoveOnlyStaleMemberWhenReservationNowBelongsToNewOrder() {
        when(reservations.get("seckill:reservation:10", "5")).thenReturn("2000");
        job.reconcile();
        verify(redis).execute(any(RedisScript.class), eq(CLEAR_KEYS), eq(MEMBER));
        verifyNoInteractions(publisher);
        verify(reservations, never()).delete(anyString(), any());
    }

    @Test
    void shouldNotConfirmMismatchedDatabaseOrder() {
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenReturn(new VoucherOrder().setId(1000L).setUserId(6L).setVoucherId(10L));
        job.reconcile();
        verify(pending, never()).remove(anyString(), any());
        verifyNoInteractions(publisher);
    }

    @Test
    void shouldIsolateMalformedMemberAndContinueOtherCandidates() throws Exception {
        when(pending.rangeByScore(eq(SECKILL_PENDING_KEY), eq(0d), anyDouble(), eq(0L), eq(100L)))
                .thenReturn(new LinkedHashSet<>(List.of("bad-member", MEMBER)));
        job.reconcile();
        verify(publisher).publish(new VoucherOrderMessage(1000L, 5L, 10L, 1));
    }

    @Test
    void shouldKeepPendingWhenDatabaseCannotBeRead() {
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenThrow(new IllegalStateException("database unavailable"));
        job.reconcile();
        verifyNoInteractions(publisher);
        verify(pending, never()).remove(anyString(), any());
    }

    @Test
    void shouldHandleEmptyBatchWithoutCallingDatabase() {
        when(pending.rangeByScore(eq(SECKILL_PENDING_KEY), eq(0d), anyDouble(), eq(0L), eq(100L)))
                .thenReturn(Set.of());
        job.reconcile();
        verifyNoInteractions(orders, reservations, publisher);
    }

    @Test
    void shouldKeepRetryBudgetAcrossRoundsAndJobInstancesEvenWhenPublishFails() throws Exception {
        AtomicLong used = new AtomicLong();
        when(redis.execute(any(RedisScript.class), eq(RETRY_KEYS), eq(MEMBER), eq("5"), eq("5"), eq("1000")))
                .thenAnswer(invocation -> used.get() >= 5 ? -1L : used.incrementAndGet());
        doThrow(new RetryException("MQ unavailable", new IllegalStateException())).when(publisher).publish(any());
        for (int i = 0; i < 8; i++) {
            new SeckillOrderReconcileJob(redis, orders, publisher, properties, redisson, new JobProperties()).reconcile();
        }
        assertEquals(5, used.get());
        verify(publisher, times(5)).publish(any());
        for (int retry = 1; retry <= 5; retry++) {
            verify(publisher).publish(new VoucherOrderMessage(1000L, 5L, 10L, retry));
        }
        verify(redis, never()).execute(any(RedisScript.class), eq(CLEAR_KEYS), eq(MEMBER));
        verify(reservations, never()).delete(anyString(), any());
    }

    @Test
    void shouldKeepCheckingCommittedOrderAfterRetryBudgetIsExhausted() {
        when(redis.execute(any(RedisScript.class), eq(RETRY_KEYS), eq(MEMBER), anyString(), eq("5"), eq("1000")))
                .thenReturn(-1L);
        job.reconcile();
        verifyNoInteractions(publisher);
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenReturn(new VoucherOrder().setId(1000L).setUserId(5L).setVoucherId(10L));
        job.reconcile();
        verify(redis).execute(any(RedisScript.class), eq(CLEAR_KEYS), eq(MEMBER));
    }

    @Test
    void shouldAllowDisablingRepublishWithoutDisablingReconciliation() {
        properties.setMaxRepublishRetries(0);
        when(redis.execute(any(RedisScript.class), eq(RETRY_KEYS), eq(MEMBER), eq("0"), eq("5"), eq("1000")))
                .thenReturn(-1L);
        job.reconcile();
        verify(orders).selectRoutedOrder(1000L, 5L, 10L);
        verifyNoInteractions(publisher);
        verifyNoInteractions(redisson);
    }

    @Test
    void shouldNotPublishIfPendingDisappearsOrCountingFails() {
        when(redis.execute(any(RedisScript.class), eq(RETRY_KEYS), eq(MEMBER), anyString(), eq("5"), eq("1000")))
                .thenReturn(0L).thenReturn((Long) null);
        job.reconcile();
        assertDoesNotThrow(job::reconcile);
        verifyNoInteractions(publisher);
    }

    @Test
    void shouldReleaseExhaustedReservationOnlyAfterLockAndFreshDatabaseRead() {
        exhaustRetries();
        when(redis.execute(any(RedisScript.class), eq(ROLLBACK_KEYS), eq("5"), eq("1000"), eq(MEMBER)))
                .thenReturn(1L);
        job.reconcile();
        var sequence = inOrder(orders, orderLock, reservations, redis);
        sequence.verify(orders).selectRoutedOrder(1000L, 5L, 10L);
        sequence.verify(reservations).get("seckill:reservation:10", "5");
        sequence.verify(orderLock).tryLock();
        sequence.verify(orders).selectRoutedOrder(1000L, 5L, 10L);
        sequence.verify(reservations).get("seckill:reservation:10", "5");
        sequence.verify(redis).execute(any(RedisScript.class), eq(ROLLBACK_KEYS), eq("5"), eq("1000"), eq(MEMBER));
        sequence.verify(orderLock).unlock();
        verifyNoInteractions(publisher);
    }

    @Test
    void shouldDeferReleaseWhenConsumerStillHoldsOrderLock() {
        exhaustRetries();
        when(orderLock.tryLock()).thenReturn(false);
        job.reconcile();
        verify(orders).selectRoutedOrder(1000L, 5L, 10L);
        verify(orderLock, never()).unlock();
        verifyNoRollback();
    }

    @Test
    void shouldKeepQualificationIfConsumerCommitsBeforeReconcilerObtainsLock() {
        exhaustRetries();
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenReturn(null,
                new VoucherOrder().setId(1000L).setUserId(5L).setVoucherId(10L));
        job.reconcile();
        verify(redis).execute(any(RedisScript.class), eq(CLEAR_KEYS), eq(MEMBER));
        verify(orderLock).unlock();
        verifyNoRollback();
    }

    @Test
    void shouldRetainPendingIfLockedDatabaseReadFailsOrReturnsConflictingOrder() {
        exhaustRetries();
        when(orders.selectRoutedOrder(1000L, 5L, 10L))
                .thenReturn(null).thenThrow(new IllegalStateException("database unavailable"))
                .thenReturn(null, new VoucherOrder().setId(1000L).setUserId(6L).setVoucherId(10L));
        assertDoesNotThrow(job::reconcile);
        assertDoesNotThrow(job::reconcile);
        verify(orderLock, times(2)).unlock();
        verify(redis, never()).execute(any(RedisScript.class), eq(CLEAR_KEYS), eq(MEMBER));
        verifyNoRollback();
    }

    @Test
    void shouldNotReleaseWhenReservationDisappearsBeforeLockedRecheck() {
        exhaustRetries();
        when(reservations.get("seckill:reservation:10", "5")).thenReturn("1000", null);
        job.reconcile();
        verify(redis, never()).execute(any(RedisScript.class), eq(CLEAR_KEYS), eq(MEMBER));
        verify(orderLock).unlock();
        verifyNoRollback();
    }

    @Test
    void shouldOnlyClearOldPendingWhenLockedReservationBelongsToNewRequest() {
        exhaustRetries();
        when(reservations.get("seckill:reservation:10", "5")).thenReturn("1000", "2000");
        job.reconcile();
        verify(redis).execute(any(RedisScript.class), eq(CLEAR_KEYS), eq(MEMBER));
        verify(orderLock).unlock();
        verifyNoRollback();
    }

    @Test
    void shouldRetryReleaseNextRoundWhenStockIsMissingOrRedisCallFails() {
        exhaustRetries();
        when(redis.execute(any(RedisScript.class), eq(ROLLBACK_KEYS), eq("5"), eq("1000"), eq(MEMBER)))
                .thenReturn(-1L, null).thenThrow(new IllegalStateException("Redis unavailable")).thenReturn(1L);
        for (int round = 0; round < 4; round++) assertDoesNotThrow(job::reconcile);
        verify(orderLock, times(4)).unlock();
        verify(redis, never()).execute(any(RedisScript.class), eq(CLEAR_KEYS), eq(MEMBER));
        verifyNoInteractions(publisher);
    }

    @Test
    void shouldNotRollbackOnLastAllowedPublishButWaitForNextClaim() throws Exception {
        when(redis.execute(any(RedisScript.class), eq(RETRY_KEYS), eq(MEMBER), eq("5"), eq("5"), eq("1000")))
                .thenReturn(5L);
        job.reconcile();
        verify(publisher).publish(new VoucherOrderMessage(1000L, 5L, 10L, 5));
        verifyNoInteractions(redisson);
        verifyNoRollback();
    }

    private void exhaustRetries() {
        when(redis.execute(any(RedisScript.class), eq(RETRY_KEYS), eq(MEMBER), eq("5"), eq("5"), eq("1000")))
                .thenReturn(-1L);
        when(orderLock.tryLock()).thenReturn(true);
    }

    private void verifyNoRollback() {
        verify(redis, never()).execute(any(RedisScript.class), eq(ROLLBACK_KEYS), anyString(), anyString(), anyString());
    }
}
