package com.hmdp.repository.redis;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.hmdp.common.constant.RedisConstants.*;
import static org.junit.jupiter.api.Assertions.*;

/** 显式开启后连接专用 Redis；只操作随机生成的订单与优惠券键。 */
@EnabledIfEnvironmentVariable(named = "HMDP_SECKILL_RESERVATION_IT", matches = "true")
class SeckillReservationIT {
    private final long voucherId = ThreadLocalRandom.current().nextLong(1_000_000_000_000L, 9_000_000_000_000L);
    private final long orderId = ThreadLocalRandom.current().nextLong(1_000_000_000_000L, 9_000_000_000_000L);
    private final long userId = 5L;
    private LettuceConnectionFactory connection;
    private StringRedisTemplate redis;
    private SeckillReservation reservation;

    @BeforeEach
    void connect() {
        connection = new LettuceConnectionFactory("127.0.0.1", Integer.getInteger("hmdp.test.redis.port", 16389));
        connection.afterPropertiesSet();
        connection.start();
        redis = new StringRedisTemplate(connection);
        redis.afterPropertiesSet();
        reservation = new SeckillReservation(redis);
        redis.opsForValue().set(SECKILL_STOCK_KEY + voucherId, "1");
    }

    @Test
    void shouldReserveAndReleaseOnlyTheMatchingOrder() {
        String member = voucherId + ":" + userId + ":" + orderId;
        assertEquals(SeckillReservation.SUCCESS, reservation.reserve(voucherId, userId, orderId));
        assertEquals("0", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        assertEquals(Long.toString(orderId), reservation.getReservedOrderId(voucherId, userId));
        assertTrue(reservation.hasTransactionMarker(orderId));
        assertEquals(member, reservation.findAcceptedRequestMember(orderId));
        assertNotNull(redis.opsForZSet().score(SECKILL_PENDING_KEY, member));

        assertEquals(SeckillReservation.DUPLICATE, reservation.reserve(voucherId, userId, orderId + 1));
        assertEquals(1L, reservation.rollback(voucherId, userId, orderId));
        assertEquals("1", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        assertNull(reservation.getReservedOrderId(voucherId, userId));
        assertNull(reservation.findAcceptedRequestMember(orderId));
        assertNull(redis.opsForZSet().score(SECKILL_PENDING_KEY, member));
        assertEquals(2L, reservation.rollback(voucherId, userId, orderId));
        assertEquals("1", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test
    void shouldNotReleaseAgainOrRemoveANewerReservation() {
        assertEquals(0L, reservation.reserve(voucherId, userId, orderId));
        assertEquals(1L, reservation.rollback(voucherId, userId, orderId));
        assertEquals(0L, reservation.reserve(voucherId, userId, orderId + 1));
        assertEquals(2L, reservation.rollback(voucherId, userId, orderId));
        assertEquals(Long.toString(orderId + 1), reservation.getReservedOrderId(voucherId, userId));
        assertEquals("0", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @Test
    void shouldNotTreatMissingReservationAsSuccessfulRelease() {
        assertEquals(0L, reservation.rollback(voucherId, userId, orderId));
        assertEquals("1", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        assertFalse(Boolean.TRUE.equals(redis.hasKey(SECKILL_RELEASED_KEY + orderId)));
    }

    @Test
    void openCircuitStopsReservationBeforeInventoryChanges() {
        redis.opsForValue().set(SECKILL_CIRCUIT_KEY, "test-dead-letter");
        try {
            assertEquals(SeckillReservation.CIRCUIT_OPEN, reservation.reserve(voucherId, userId, orderId));
            assertEquals("1", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
            assertNull(reservation.findAcceptedRequestMember(orderId));
            assertNull(reservation.getReservedOrderId(voucherId, userId));
        } finally {
            redis.delete(SECKILL_CIRCUIT_KEY);
        }
    }

    @Test
    void canBackfillOldPendingRequestsForDirectOrderLookup() {
        String pendingKey = SECKILL_PENDING_KEY + ":migration-it:" + orderId;
        String requestKey = SECKILL_REQUESTS_KEY + ":migration-it:" + orderId;
        String member = voucherId + ":" + userId + ":" + orderId;
        var script = new DefaultRedisScript<String>();
        script.setLocation(new ClassPathResource("seckill-request-index-backfill.lua"));
        script.setResultType(String.class);
        try {
            redis.opsForZSet().add(pendingKey, member, System.currentTimeMillis());
            String cursor = "0";
            do {
                cursor = redis.execute(script, List.of(pendingKey, requestKey), cursor, "10");
            } while (!"0".equals(cursor));
            assertEquals(member, redis.opsForHash().get(requestKey, Long.toString(orderId)));
            assertEquals("0", redis.execute(script, List.of(pendingKey, requestKey), "0", "10"));
        } finally {
            redis.delete(List.of(pendingKey, requestKey));
        }
    }

    @Test
    void shouldNotIncreaseStockTwiceWhenCleanupFailsAfterRelease() {
        String pendingKey = SECKILL_PENDING_KEY + ":it:" + orderId;
        String retryKey = SECKILL_PENDING_RETRIES_KEY + ":it:" + orderId;
        String member = voucherId + ":" + userId + ":" + orderId;
        List<String> keys = List.of(SECKILL_STOCK_KEY + voucherId, SECKILL_RESERVATION_KEY + voucherId,
                pendingKey, retryKey, SECKILL_RELEASED_KEY + orderId, SECKILL_REQUESTS_KEY);
        var script = new DefaultRedisScript<Long>();
        script.setLocation(new ClassPathResource("seckill-rollback.lua"));
        script.setResultType(Long.class);
        try {
            redis.opsForValue().set(SECKILL_STOCK_KEY + voucherId, "0");
            redis.opsForHash().put(SECKILL_RESERVATION_KEY + voucherId, Long.toString(userId), Long.toString(orderId));
            redis.opsForValue().set(pendingKey, "wrong-type");
            assertThrows(Exception.class, () -> redis.execute(script, keys,
                    Long.toString(userId), Long.toString(orderId), member));
            assertEquals("1", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
            assertEquals(member, redis.opsForValue().get(SECKILL_RELEASED_KEY + orderId));
            redis.delete(pendingKey);
            assertEquals(2L, redis.execute(script, keys, Long.toString(userId), Long.toString(orderId), member));
            assertEquals("1", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        } finally {
            redis.delete(List.of(pendingKey, retryKey));
        }
    }

    @AfterEach
    void disconnect() {
        if (redis != null) {
            String member = voucherId + ":" + userId + ":" + orderId;
            redis.delete(List.of(SECKILL_STOCK_KEY + voucherId, SECKILL_RESERVATION_KEY + voucherId,
                    SECKILL_TXN_KEY + orderId, SECKILL_TXN_KEY + (orderId + 1),
                    SECKILL_RELEASED_KEY + orderId, SECKILL_RELEASED_KEY + (orderId + 1)));
            redis.opsForZSet().remove(SECKILL_PENDING_KEY, member, voucherId + ":" + userId + ":" + (orderId + 1));
            redis.opsForHash().delete(SECKILL_PENDING_RETRIES_KEY, member);
            redis.opsForHash().delete(SECKILL_REQUESTS_KEY, Long.toString(orderId), Long.toString(orderId + 1));
        }
        if (connection != null) connection.destroy();
    }
}
