package com.hmdp.cache;

import cn.hutool.json.JSONUtil;
import com.hmdp.cache.config.CacheWarmupProperties;
import com.hmdp.entity.Shop;
import com.hmdp.entity.Voucher;
import com.hmdp.framework.cache.LocalCacheClient;
import com.hmdp.framework.cache.RedisData;
import com.hmdp.framework.cache.config.LocalCacheProperties;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.mapper.VoucherMapper;
import org.mockito.ArgumentCaptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static com.hmdp.common.constant.RedisConstants.CACHE_WARMUP_SHOP_IDS_KEY;

class CacheWarmupServiceTest {
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final ValueOperations<String, String> values = mock(ValueOperations.class);
    private final ShopMapper shops = mock(ShopMapper.class);
    private final ShopTypeCacheService types = mock(ShopTypeCacheService.class);
    private final VoucherListCacheService vouchers = mock(VoucherListCacheService.class);
    private final CacheWarmupProperties properties = new CacheWarmupProperties();
    private final VoucherMapper voucherMapper = mock(VoucherMapper.class);
    private final CacheWarmupService service = new CacheWarmupService(types, vouchers, shops, redis, properties, voucherMapper);

    @BeforeEach
    void setup() { when(redis.opsForValue()).thenReturn(values); }

    @Test
    void shouldSelectActivityShopsWithOneTimeWindowAndConfiguredLimit() {
        properties.setAheadOfStart(Duration.ofMinutes(20));
        properties.setMaxShops(50);
        when(voucherMapper.findWarmupShopIds(any(), any(), eq(50))).thenReturn(List.of(1L, 2L));
        assertEquals(List.of(1L, 2L), service.findShopIdsToWarm());
        var now = ArgumentCaptor.forClass(LocalDateTime.class);
        var before = ArgumentCaptor.forClass(LocalDateTime.class);
        verify(voucherMapper).findWarmupShopIds(now.capture(), before.capture(), eq(50));
        assertEquals(Duration.ofMinutes(20), Duration.between(now.getValue(), before.getValue()));
    }

    @Test
    void shouldAllowExplicitOverrideWithoutQueryingActivities() {
        properties.setHotShopIds(List.of(2L, 1L, 2L));
        assertEquals(List.of(2L, 1L), service.findShopIdsToWarm());
        verifyNoInteractions(voucherMapper);
    }

    @Test
    void shouldPublishOneExpiringSnapshotAndKeepSnowflakeIdsExact() {
        long id = 9_007_199_254_740_993L;
        service.publishShopIds(List.of(id));
        var json = ArgumentCaptor.forClass(String.class);
        var expiration = ArgumentCaptor.forClass(org.springframework.data.redis.core.types.Expiration.class);
        verify(values).set(eq(CACHE_WARMUP_SHOP_IDS_KEY), json.capture(), expiration.capture());
        assertEquals(300_000, expiration.getValue().getExpirationTimeInMilliseconds());
        when(values.get(CACHE_WARMUP_SHOP_IDS_KEY)).thenReturn(json.getValue());
        assertEquals(List.of(id), service.loadPublishedShopIds());
        verify(redis, never()).delete(anyString());
        verifyNoInteractions(shops, voucherMapper);
    }

    @Test
    void shouldReplaceOldSnapshotWithEmptyListAndDistinguishMissingSnapshot() {
        service.publishShopIds(List.of());
        verify(values).set(eq(CACHE_WARMUP_SHOP_IDS_KEY), eq("[]"), any(org.springframework.data.redis.core.types.Expiration.class));
        when(values.get(CACHE_WARMUP_SHOP_IDS_KEY)).thenReturn("[]", null);
        assertEquals(List.of(), service.loadPublishedShopIds());
        assertThrows(IllegalStateException.class, service::loadPublishedShopIds);
        verifyNoInteractions(shops, voucherMapper);
    }

    @Test
    void shouldWriteCompatibleLogicalShopCacheAndSkipFreshEntry() {
        when(shops.selectById(1L)).thenReturn(new Shop().setId(1L).setName("shop"));
        service.warmShopRedis(1L);
        var json = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(values).set(eq("cache:shop:1"), json.capture());
        RedisData cache = JSONUtil.toBean(json.getValue(), RedisData.class);
        assertNotNull(cache.getExpireTime());
        when(values.get("cache:shop:1")).thenReturn(json.getValue());
        service.warmShopRedis(1L);
        verify(shops, times(1)).selectById(1L);
        verifyNoMoreInteractions(shops);
        verify(values, never()).increment(anyString());
        verify(redis, never()).opsForHash();
    }

    @Test
    void shouldFailLocalWarmupWhenRedisDataMissingWithoutQueryingDatabase() {
        when(vouchers.warmLocalFromRedis(1L)).thenReturn(false);
        assertThrows(IllegalStateException.class, () -> service.warmVouchersLocal(1L));
        verifyNoInteractions(shops, values);
        verify(vouchers, never()).getVoucherByShopId(anyLong());
    }

    @Test
    void shouldReplaceExistingLocalValueFromRedisWithoutDbFallback() {
        var properties = new LocalCacheProperties();
        AtomicInteger queries = new AtomicInteger();
        var cache = new LocalCacheClient<Long, Voucher>(redis, properties, Voucher.class, id -> "list:" + id,
                id -> { queries.incrementAndGet(); return List.of(); });
        when(values.get("list:1")).thenReturn("[{\"id\":1,\"title\":\"old\"}]", "[{\"id\":1,\"title\":\"new\"}]");
        assertEquals("old", cache.get(1L).getFirst().getTitle());
        assertTrue(cache.warmLocalFromRedis(1L));
        assertEquals("new", cache.get(1L).getFirst().getTitle());
        assertEquals(0, queries.get());
        when(values.get("list:1")).thenReturn(null);
        assertFalse(cache.warmLocalFromRedis(1L));
        assertEquals("new", cache.get(1L).getFirst().getTitle());
        assertEquals(0, queries.get());
    }

    @Test
    void shouldWarmOnlyMissingOrExpiringRedisAndRespectMysqlMode() {
        var properties = new LocalCacheProperties();
        AtomicInteger queries = new AtomicInteger();
        var cache = new LocalCacheClient<Long, Voucher>(redis, properties, Voucher.class, id -> "list:" + id,
                id -> { queries.incrementAndGet(); return List.of(); });
        when(redis.getExpire("list:1", TimeUnit.MILLISECONDS)).thenReturn(600_000L, -2L);
        cache.warmRedis(1L, Duration.ofMinutes(2));
        assertEquals(0, queries.get());
        cache.warmRedis(1L, Duration.ofMinutes(2));
        assertEquals(1, queries.get());
        verify(values).set(eq("list:1"), eq("[]"), any(org.springframework.data.redis.core.types.Expiration.class));
        clearInvocations(redis, values);
        properties.setMode("mysql");
        cache.warmRedis(1L, Duration.ofMinutes(2));
        assertTrue(cache.warmLocalFromRedis(1L));
        verifyNoInteractions(redis, values);
    }
}
