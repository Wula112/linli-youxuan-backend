package com.hmdp.job;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/** 显式开启后连接专用Redis测试端口，只读写本测试随机前缀下的Key。 */
@EnabledIfEnvironmentVariable(named = "HMDP_SECKILL_REDIS_IT", matches = "true")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SeckillPendingRetryIT {

    private static final String MEMBER = "10:5:1000";
    private final String prefix = "it:seckill:retry:" + UUID.randomUUID() + ":";
    private final String pending = prefix + "pending";
    private final String retries = prefix + "retries";
    private final String reservation = prefix + "reservation";
    private final String stock = prefix + "stock";
    private LettuceConnectionFactory connection;
    private StringRedisTemplate redis;

    @BeforeAll
    void connect() {
        connection = new LettuceConnectionFactory("127.0.0.1", Integer.getInteger("hmdp.test.redis.port", 6388));
        connection.afterPropertiesSet();
        connection.start();
        redis = new StringRedisTemplate(connection);
    }

    @BeforeEach
    void reserve() {
        redis.opsForZSet().add(pending, MEMBER, 1);
        redis.opsForHash().put(reservation, "5", "1000");
        redis.opsForValue().set(stock, "0");
    }

    @AfterEach
    void clear() {
        redis.delete(List.of(pending, retries, reservation, stock));
    }

    @AfterAll
    void disconnect() {
        connection.destroy();
    }

    @Test
    void shouldRetainBudgetAcrossClientsAndRoundsWithoutExpiring() {
        for (int round = 1; round <= 5; round++) {
            StringRedisTemplate anotherClient = new StringRedisTemplate(connection);
            assertEquals((long) round, anotherClient.execute(script("seckill-pending-retry.lua"),
                    List.of(pending, retries, reservation), MEMBER, "5", "5", "1000"));
        }
        assertEquals(-1L, retry(5));
        assertEquals(-1L, retry(5));
        assertEquals("5", redis.opsForHash().get(retries, MEMBER));
        assertEquals(-1L, redis.getExpire(retries));
        assertNotNull(redis.opsForZSet().score(pending, MEMBER));
        assertEquals("1000", redis.opsForHash().get(reservation, "5"));
        assertEquals("0", redis.opsForValue().get(stock));
    }

    @Test
    void shouldNotExceedBudgetWhenManyInstancesRetryConcurrently() throws Exception {
        try (ExecutorService workers = Executors.newFixedThreadPool(12)) {
            List<Future<Long>> attempts = IntStream.range(0, 64)
                    .mapToObj(i -> workers.submit(() -> retry(5))).toList();
            int granted = 0;
            for (Future<Long> attempt : attempts) {
                if (attempt.get(5, TimeUnit.SECONDS) > 0) granted++;
            }
            assertEquals(5, granted);
        }
        assertEquals("5", redis.opsForHash().get(retries, MEMBER));
    }

    @Test
    void shouldNotLeaveOrRecreateCounterWhenCleanupRacesWithRetry() throws Exception {
        String other = "10:6:1001";
        redis.opsForHash().put(retries, other, "2");
        try (ExecutorService workers = Executors.newFixedThreadPool(12)) {
            List<Future<Long>> actions = IntStream.range(0, 64).mapToObj(i -> workers.submit(() ->
                    i == 32 ? redis.execute(script("seckill-pending-clear.lua"), List.of(pending, retries), MEMBER)
                            : retry(5))).toList();
            for (Future<Long> action : actions) action.get(5, TimeUnit.SECONDS);
        }
        assertNull(redis.opsForZSet().score(pending, MEMBER));
        assertNull(redis.opsForHash().get(retries, MEMBER));
        assertEquals("2", redis.opsForHash().get(retries, other));
        assertEquals(0L, retry(5));
        assertEquals("1000", redis.opsForHash().get(reservation, "5"));
    }

    @Test
    void shouldNotConsumeBudgetWhenDisabledOrReservationNoLongerMatches() {
        assertEquals(-1L, retry(0));
        assertFalse(Boolean.TRUE.equals(redis.hasKey(retries)));
        redis.opsForHash().put(reservation, "5", "2000");
        assertEquals(0L, retry(5));
        assertFalse(Boolean.TRUE.equals(redis.hasKey(retries)));
    }

    @Test
    void shouldClearCounterDuringExistingRollbackWithoutChangingItsOwnershipRules() {
        assertEquals(1L, retry(5));
        List<String> keys = List.of(stock, reservation, pending, retries);
        assertEquals(1L, redis.execute(script("seckill-rollback.lua"), keys, "5", "1000", MEMBER));
        assertEquals("1", redis.opsForValue().get(stock));
        assertNull(redis.opsForHash().get(retries, MEMBER));
        assertNull(redis.opsForZSet().score(pending, MEMBER));
        assertEquals(0L, redis.execute(script("seckill-rollback.lua"), keys, "5", "1000", MEMBER));
        assertEquals("1", redis.opsForValue().get(stock));
        // 旧订单的清理不能影响新预占及其补发次数。
        redis.opsForHash().put(reservation, "5", "2000");
        redis.opsForHash().put(retries, "10:5:2000", "2");
        assertEquals(0L, redis.execute(script("seckill-rollback.lua"), keys, "5", "1000", MEMBER));
        assertEquals("2000", redis.opsForHash().get(reservation, "5"));
        assertEquals("2", redis.opsForHash().get(retries, "10:5:2000"));
    }

    private Long retry(int max) {
        return redis.execute(script("seckill-pending-retry.lua"), List.of(pending, retries, reservation),
                MEMBER, Integer.toString(max), "5", "1000");
    }

    private DefaultRedisScript<Long> script(String name) {
        var script = new DefaultRedisScript<Long>();
        script.setLocation(new ClassPathResource(name));
        script.setResultType(Long.class);
        return script;
    }
}
