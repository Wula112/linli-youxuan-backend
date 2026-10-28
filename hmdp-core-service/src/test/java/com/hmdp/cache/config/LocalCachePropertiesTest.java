package com.hmdp.cache.config;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.cache.VoucherListCacheService;
import com.hmdp.cache.ShopTypeCacheService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class LocalCachePropertiesTest {

    private final VoucherMapper voucherMapper = mock(VoucherMapper.class);
    private final ShopTypeMapper shopTypeMapper = mock(ShopTypeMapper.class);
    private final StringRedisTemplate stringRedisTemplate = mock(StringRedisTemplate.class);
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(CacheTestConfiguration.class)
            .withBean(VoucherMapper.class, () -> voucherMapper)
            .withBean(ShopTypeMapper.class, () -> shopTypeMapper)
            .withBean(StringRedisTemplate.class, () -> stringRedisTemplate);

    @Test
    void shouldInitializeDefaultCacheWithoutRequestingRedisOrDatabase() {
        contextRunner.run(context -> {
            assertNull(context.getStartupFailure());
            VoucherCacheProperties properties = context.getBean(VoucherCacheProperties.class);
            assertEquals("caffeine", properties.getMode());
            assertEquals(Duration.ofSeconds(5), properties.getRefreshAfterWrite());
            assertEquals(Duration.ofMinutes(10), properties.getExpireAfterWrite());
            assertEquals(Duration.ofMinutes(10), properties.getRedisTtl());
            assertEquals(1000, properties.getMaximumSize());
            ShopTypeCacheProperties shopProperties = context.getBean(ShopTypeCacheProperties.class);
            assertEquals("caffeine", shopProperties.getMode());
            assertEquals(1, shopProperties.getMaximumSize());
            assertEquals(Duration.ofSeconds(5), shopProperties.getRefreshAfterWrite());
            assertEquals(Duration.ofMinutes(10), shopProperties.getExpireAfterWrite());
            assertEquals(Duration.ofMinutes(30), shopProperties.getRedisTtl());
            assertEquals(0, getLocalCache(context.getBean(VoucherListCacheService.class), "voucherListCache").estimatedSize());
            assertEquals(0, getLocalCache(context.getBean(ShopTypeCacheService.class), "shopTypeCache").estimatedSize());
            verifyNoInteractions(voucherMapper, shopTypeMapper);
            verify(stringRedisTemplate, never()).opsForValue();
        });
    }

    @Test
    void shouldBindIndependentModesAndDurationsBeforeCacheInitialization() {
        contextRunner.withPropertyValues(
                "hmdp.cache.voucher-list.mode=ReDiS",
                "hmdp.cache.voucher-list.maximum-size=20",
                "hmdp.cache.voucher-list.refresh-after-write=250ms",
                "hmdp.cache.voucher-list.expire-after-write=3m",
                "hmdp.cache.voucher-list.redis-ttl=90s",
                "hmdp.cache.shop-type.mode=MySQL",
                "hmdp.cache.shop-type.maximum-size=2",
                "hmdp.cache.shop-type.refresh-after-write=20s",
                "hmdp.cache.shop-type.expire-after-write=5m",
                "hmdp.cache.shop-type.redis-ttl=12m"
        ).run(context -> {
            assertNull(context.getStartupFailure());
            VoucherCacheProperties properties = context.getBean(VoucherCacheProperties.class);
            assertEquals("ReDiS", properties.getMode());
            assertEquals(20, properties.getMaximumSize());
            assertEquals(Duration.ofMillis(250), properties.getRefreshAfterWrite());
            assertEquals(Duration.ofMinutes(3), properties.getExpireAfterWrite());
            assertEquals(Duration.ofSeconds(90), properties.getRedisTtl());
            LoadingCache<?, ?> cache = getLocalCache(context.getBean(VoucherListCacheService.class), "voucherListCache");
            assertEquals(20, cache.policy().eviction().orElseThrow().getMaximum());
            assertEquals(250, cache.policy().refreshAfterWrite().orElseThrow().getRefreshesAfter(TimeUnit.MILLISECONDS));
            assertEquals(3, cache.policy().expireAfterWrite().orElseThrow().getExpiresAfter(TimeUnit.MINUTES));
            ShopTypeCacheProperties shopProperties = context.getBean(ShopTypeCacheProperties.class);
            assertEquals("MySQL", shopProperties.getMode());
            assertEquals(2, shopProperties.getMaximumSize());
            assertEquals(Duration.ofSeconds(20), shopProperties.getRefreshAfterWrite());
            assertEquals(Duration.ofMinutes(5), shopProperties.getExpireAfterWrite());
            assertEquals(Duration.ofMinutes(12), shopProperties.getRedisTtl());
            LoadingCache<?, ?> shopCache = getLocalCache(context.getBean(ShopTypeCacheService.class), "shopTypeCache");
            assertEquals(2, shopCache.policy().eviction().orElseThrow().getMaximum());
            assertEquals(20, shopCache.policy().refreshAfterWrite().orElseThrow().getRefreshesAfter(TimeUnit.SECONDS));
            assertEquals(5, shopCache.policy().expireAfterWrite().orElseThrow().getExpiresAfter(TimeUnit.MINUTES));
            verifyNoInteractions(voucherMapper, shopTypeMapper);
            verify(stringRedisTemplate, never()).opsForValue();
        });
    }

    @ParameterizedTest
    @MethodSource("invalidProperties")
    void shouldRejectInvalidConfigurationBeforeAnyDataAccess(String property) {
        contextRunner.withPropertyValues(property).run(context -> {
            assertNotNull(context.getStartupFailure());
            verifyNoInteractions(voucherMapper, shopTypeMapper);
            verify(stringRedisTemplate, never()).opsForValue();
        });
    }

    private static Stream<String> invalidProperties() {
        return Stream.of("voucher-list", "shop-type").flatMap(prefix ->
                Stream.of("mode=unknown", "mode=", "maximum-size=0", "redis-ttl=0s",
                                "refresh-after-write=0s", "expire-after-write=-1s")
                        .map(property -> "hmdp.cache." + prefix + "." + property));
    }

    private LoadingCache<?, ?> getLocalCache(Object service, String field) {
        Object cacheClient = ReflectionTestUtils.getField(service, field);
        assertNotNull(cacheClient);
        return (LoadingCache<?, ?>) ReflectionTestUtils.getField(cacheClient, "localCache");
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties({VoucherCacheProperties.class, ShopTypeCacheProperties.class})
    @Import({VoucherListCacheService.class, ShopTypeCacheService.class})
    static class CacheTestConfiguration {
    }
}
