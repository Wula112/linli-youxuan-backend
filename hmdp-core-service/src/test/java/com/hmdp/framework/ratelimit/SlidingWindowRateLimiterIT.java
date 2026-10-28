package com.hmdp.framework.ratelimit;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.mock.env.MockEnvironment;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/** 显式开启后只访问专用Redis测试端口，所有Key使用随机前缀并在测试后清理。 */
@EnabledIfEnvironmentVariable(named = "HMDP_RATE_LIMIT_IT", matches = "true")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SlidingWindowRateLimiterIT {

    private final DefaultRedisScript<Long> script = new DefaultRedisScript<>();
    private final Set<String> keys = new HashSet<>();
    private LettuceConnectionFactory connection;
    private StringRedisTemplate redis;

    @BeforeAll
    void connect() {
        script.setLocation(new ClassPathResource("limiter.lua"));
        script.setResultType(Long.class);
        connection = new LettuceConnectionFactory("127.0.0.1", Integer.getInteger("hmdp.test.redis.port", 6388));
        connection.afterPropertiesSet();
        connection.start();
        redis = new StringRedisTemplate(connection);
    }

    @AfterEach
    void clearKeys() {
        if (!keys.isEmpty()) redis.delete(keys);
        keys.clear();
    }

    @AfterAll
    void disconnect() {
        connection.destroy();
    }

    @Test
    void shouldShareOneBudgetAcrossConcurrentApplicationInstances() throws Exception {
        String prefix = "it:rate:" + UUID.randomUUID();
        String key = "rate:limit:" + prefix + ":" + Endpoint.class.getName() + ":request()";
        keys.add(key);
        AtomicInteger businessCalls = new AtomicInteger();
        Endpoint first = endpoint(prefix, businessCalls, redis);
        Endpoint second = endpoint(prefix, businessCalls, new StringRedisTemplate(connection));
        try (ExecutorService workers = Executors.newFixedThreadPool(16)) {
            CountDownLatch start = new CountDownLatch(1);
            var attempts = IntStream.range(0, 64).mapToObj(i -> workers.submit(() -> {
                start.await();
                try {
                    (i % 2 == 0 ? first : second).request();
                    return true;
                } catch (RateLimitException ex) {
                    return false;
                }
            })).toList();
            start.countDown();
            int accepted = 0;
            for (Future<Boolean> attempt : attempts) if (attempt.get(10, TimeUnit.SECONDS)) accepted++;
            assertEquals(20, accepted);
        }
        assertEquals(20, businessCalls.get());
        assertEquals(20L, redis.opsForZSet().zCard(key));
    }

    @Test
    void shouldRecordEveryConcurrentRequestWithoutMemberOverwrite() throws Exception {
        String key = key();
        try (ExecutorService workers = Executors.newFixedThreadPool(16)) {
            var attempts = IntStream.range(0, 64).mapToObj(i -> workers.submit(() -> acquire(key, 10000, 100))).toList();
            Set<Long> counts = new HashSet<>();
            for (Future<Long> attempt : attempts) counts.add(attempt.get(10, TimeUnit.SECONDS));
            assertEquals(64, counts.size());
        }
        assertEquals(64L, redis.opsForZSet().zCard(key));
    }

    @Test
    void shouldSlideByRemovingOnlyExpiredRequestsAndKeepRecentOnes() {
        String key = key();
        long now = serverTime();
        redis.opsForZSet().add(key, "older", now - 4000);
        redis.opsForZSet().add(key, "recent", now - 1000);
        assertEquals(0L, acquire(key, 5000, 2));
        redis.opsForZSet().add(key, "older", now - 6000);
        assertEquals(2L, acquire(key, 5000, 2));
        assertNull(redis.opsForZSet().score(key, "older"));
        assertNotNull(redis.opsForZSet().score(key, "recent"));
        assertEquals(2L, redis.opsForZSet().zCard(key));
    }

    @Test
    void shouldSupportSubSecondWindowsAndExpireIdleKeys() {
        String key = key();
        assertEquals(1L, acquire(key, 500, 1));
        Long ttl = redis.getExpire(key, TimeUnit.MILLISECONDS);
        assertNotNull(ttl);
        assertTrue(ttl > 0 && ttl <= 500);
        assertEquals(0L, acquire(key, 500, 1));
        await().atMost(Duration.ofSeconds(3)).until(() -> !Boolean.TRUE.equals(redis.hasKey(key)));
        assertEquals(1L, acquire(key, 500, 1));
    }

    @Test
    void shouldNotExtendExpirationWhenRequestsAreRejected() {
        String key = key();
        assertEquals(1L, acquire(key, 5000, 1));
        redis.expire(key, Duration.ofMillis(200));
        assertEquals(0L, acquire(key, 5000, 1));
        assertTrue(redis.getExpire(key, TimeUnit.MILLISECONDS) <= 200);
    }

    @Test
    void shouldRejectInvalidParametersWithoutWritingKey() {
        String key = key();
        assertThrows(RuntimeException.class, () -> redis.execute(script, List.of(key), "0", "1", "request"));
        assertThrows(RuntimeException.class, () -> redis.execute(script, List.of(key), "1000", "1.5", "request"));
        assertFalse(Boolean.TRUE.equals(redis.hasKey(key)));
    }

    private Endpoint endpoint(String prefix, AtomicInteger businessCalls, StringRedisTemplate client) {
        MockEnvironment environment = new MockEnvironment().withProperty("test.rate.key", prefix);
        AspectJProxyFactory factory = new AspectJProxyFactory(new Endpoint(businessCalls));
        factory.addAspect(new RateLimiterAspect(client, environment));
        return factory.getProxy();
    }

    private String key() {
        String key = "it:rate:" + UUID.randomUUID();
        keys.add(key);
        return key;
    }

    private Long acquire(String key, long window, int limit) {
        return redis.execute(script, List.of(key), Long.toString(window), Integer.toString(limit), UUID.randomUUID().toString());
    }

    private long serverTime() {
        return redis.execute(new DefaultRedisScript<>("local t=redis.call('TIME'); "
                + "return tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000)", Long.class), List.of());
    }

    public static class Endpoint {
        private final AtomicInteger businessCalls;

        public Endpoint(AtomicInteger businessCalls) {
            this.businessCalls = businessCalls;
        }

        @RateLimiter(key = "${test.rate.key}", window = "10s", limit = "20")
        public int request() {
            return businessCalls.incrementAndGet();
        }
    }
}
