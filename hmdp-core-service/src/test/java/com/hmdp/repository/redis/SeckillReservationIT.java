package com.hmdp.repository.redis;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

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
        assertNotNull(redis.opsForZSet().score(SECKILL_PENDING_KEY, member));

        assertEquals(SeckillReservation.DUPLICATE, reservation.reserve(voucherId, userId, orderId + 1));
        assertEquals(1L, reservation.rollback(voucherId, userId, orderId));
        assertEquals("1", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
        assertNull(reservation.getReservedOrderId(voucherId, userId));
        assertNull(redis.opsForZSet().score(SECKILL_PENDING_KEY, member));
        assertEquals(0L, reservation.rollback(voucherId, userId, orderId));
        assertEquals("1", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
    }

    @AfterEach
    void disconnect() {
        if (redis != null) {
            String member = voucherId + ":" + userId + ":" + orderId;
            redis.delete(List.of(SECKILL_STOCK_KEY + voucherId, SECKILL_RESERVATION_KEY + voucherId,
                    SECKILL_TXN_KEY + orderId, SECKILL_TXN_KEY + (orderId + 1)));
            redis.opsForZSet().remove(SECKILL_PENDING_KEY, member);
            redis.opsForHash().delete(SECKILL_PENDING_RETRIES_KEY, member);
        }
        if (connection != null) connection.destroy();
    }
}
