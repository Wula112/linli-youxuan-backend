package com.hmdp.repository.redis;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SeckillReservationTest {

    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final SeckillReservation reservation = new SeckillReservation(redis);

    @Test
    void shouldDeductAndWriteTransactionMarkerInOneScript() {
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class))).thenReturn(0L);

        assertEquals(SeckillReservation.SUCCESS, reservation.reserve(10L, 5L, 1000L));

        verify(redis).execute(any(RedisScript.class),
                eq(List.of("seckill:stock:10", "seckill:reservation:10", "seckill:pending", "seckill:txn:1000",
                        "seckill:requests", "seckill:order:circuit")),
                eq("5"), eq("1000"), eq("10"), anyString(), eq("3600"));
    }

    @Test
    void shouldRejectEmptyScriptResult() {
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class))).thenReturn(null);
        assertThrows(IllegalStateException.class, () -> reservation.reserve(10L, 5L, 1000L));
    }

    @Test
    void shouldReadTransactionMarkerByOrderId() {
        when(redis.hasKey("seckill:txn:1000")).thenReturn(true);
        assertTrue(reservation.hasTransactionMarker(1000L));

        when(redis.hasKey("seckill:txn:1000")).thenReturn(false);
        assertFalse(reservation.hasTransactionMarker(1000L));
    }

    @Test
    void shouldRollbackOnlyItsOwnReservationAndPending() {
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class))).thenReturn(1L);

        assertEquals(1L, reservation.rollback(10L, 5L, 1000L));

        verify(redis).execute(any(RedisScript.class),
                eq(List.of("seckill:stock:10", "seckill:reservation:10", "seckill:pending",
                        "seckill:pending:retries", "seckill:released:1000", "seckill:requests")),
                eq("5"), eq("1000"), eq("10:5:1000"));
    }

    @Test
    void shouldRejectInvalidOrderIdentityBeforeRollback() {
        assertThrows(IllegalArgumentException.class, () -> reservation.rollback(10L, 5L, null));
        assertThrows(IllegalArgumentException.class, () -> reservation.rollback(10L, 0L, 1000L));
        assertThrows(IllegalArgumentException.class, () -> reservation.rollback(-1L, 5L, 1000L));
        verifyNoInteractions(redis);
    }

    @Test
    void shouldClearPendingWithItsRetryCounter() {
        reservation.clearPending("10:5:1000");

        verify(redis).execute(any(RedisScript.class),
                eq(List.of("seckill:pending", "seckill:pending:retries", "seckill:requests")),
                eq("10:5:1000"), eq("1000"));
    }
}
