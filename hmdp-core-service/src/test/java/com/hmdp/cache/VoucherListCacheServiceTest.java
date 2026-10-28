package com.hmdp.cache;

import cn.hutool.json.JSONUtil;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.hmdp.cache.config.VoucherCacheProperties;
import com.hmdp.entity.Voucher;
import com.hmdp.mapper.VoucherMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hmdp.common.constant.RedisConstants.*;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class VoucherListCacheServiceTest {

    @Mock
    private StringRedisTemplate stringRedisTemplate;
    @Mock
    private ValueOperations<String, String> valueOperations;
    @Mock
    private VoucherMapper voucherMapper;

    private LoadingCache<Long, ?> voucherListCache;
    private VoucherCacheProperties cacheProperties;
    private VoucherListCacheService cacheService;

    @BeforeEach
    void setUp() {
        cacheProperties = new VoucherCacheProperties();
        cacheService = new VoucherListCacheService();
        ReflectionTestUtils.setField(cacheService, "voucherMapper", voucherMapper);
        ReflectionTestUtils.setField(cacheService, "stringRedisTemplate", stringRedisTemplate);
        ReflectionTestUtils.setField(cacheService, "cacheProperties", cacheProperties);
        cacheService.initCache();
        Object cacheClient = ReflectionTestUtils.getField(cacheService, "voucherListCache");
        voucherListCache = (LoadingCache<Long, ?>)
                ReflectionTestUtils.getField(cacheClient, "localCache");
    }

    @Test
    void shouldReturnLocalCacheWithoutAccessingRedisOrDatabase() {
        List<Voucher> vouchers = List.of(new Voucher().setId(10L).setShopId(1L).setTitle("代金券"));
        cacheProperties.setMode("CaFfEiNe");
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(CACHE_SHOP_VOUCHER_KEY + 1L)).thenReturn(JSONUtil.toJsonStr(vouchers));
        cacheService.getVoucherByShopId(1L);
        clearInvocations(stringRedisTemplate, valueOperations);

        assertEquals(vouchers, cacheService.getVoucherByShopId(1L));
        verifyNoInteractions(stringRedisTemplate, voucherMapper);
    }

    @Test
    void shouldPopulateLocalCacheFromRedisAndPreserveVoucherFields() {
        Voucher voucher = new Voucher().setId(10L).setShopId(1L).setTitle("秒杀代金券")
                .setRules("到店使用").setPayValue(5000L).setActualValue(10000L).setStock(20)
                .setBeginTime(LocalDateTime.of(2026, 10, 18, 10, 0))
                .setEndTime(LocalDateTime.of(2026, 10, 19, 10, 0));
        List<Voucher> vouchers = List.of(voucher);
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(CACHE_SHOP_VOUCHER_KEY + 1L)).thenReturn(JSONUtil.toJsonStr(vouchers));

        assertEquals(vouchers, cacheService.getVoucherByShopId(1L));
        assertEquals(vouchers, cacheService.getVoucherByShopId(1L));

        assertNotNull(voucherListCache.getIfPresent(1L));
        verify(valueOperations, times(1)).get(CACHE_SHOP_VOUCHER_KEY + 1L);
        verifyNoInteractions(voucherMapper);
        verify(valueOperations, never()).set(anyString(), anyString(), any(Expiration.class));
    }

    @Test
    void shouldPopulateBothCachesOnDatabaseHit() {
        cacheProperties.setRedisTtl(Duration.ofSeconds(45));
        List<Voucher> vouchers = List.of(new Voucher().setId(10L).setShopId(1L).setTitle("代金券"));
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(voucherMapper.queryVoucherOfShop(1L)).thenReturn(vouchers);

        assertEquals(vouchers, cacheService.getVoucherByShopId(1L));
        assertEquals(vouchers, cacheService.getVoucherByShopId(1L));

        verify(voucherMapper, times(1)).queryVoucherOfShop(1L);
        verify(valueOperations, times(1)).get(CACHE_SHOP_VOUCHER_KEY + 1L);
        ArgumentCaptor<Expiration> expiration = ArgumentCaptor.forClass(Expiration.class);
        verify(valueOperations).set(eq(CACHE_SHOP_VOUCHER_KEY + 1L), eq(JSONUtil.toJsonStr(vouchers)), expiration.capture());
        assertEquals(45, expiration.getValue().getExpirationTimeInSeconds());
        assertNotNull(voucherListCache.getIfPresent(1L));
    }

    @Test
    void shouldKeepDifferentShopsVoucherListsSeparate() {
        List<Voucher> firstShop = List.of(new Voucher().setId(10L).setShopId(1L));
        List<Voucher> secondShop = List.of(new Voucher().setId(20L).setShopId(2L));
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(voucherMapper.queryVoucherOfShop(1L)).thenReturn(firstShop);
        when(voucherMapper.queryVoucherOfShop(2L)).thenReturn(secondShop);

        assertEquals(firstShop, cacheService.getVoucherByShopId(1L));
        assertEquals(secondShop, cacheService.getVoucherByShopId(2L));
        assertEquals(firstShop, cacheService.getVoucherByShopId(1L));

        verify(voucherMapper, times(1)).queryVoucherOfShop(1L);
        verify(voucherMapper, times(1)).queryVoucherOfShop(2L);
        assertNotNull(voucherListCache.getIfPresent(1L));
        assertNotNull(voucherListCache.getIfPresent(2L));
    }

    @Test
    void shouldCacheEmptyListsInBothLevels() {
        String key = CACHE_SHOP_VOUCHER_KEY + 1L;
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(key)).thenReturn(null, "[]");
        when(voucherMapper.queryVoucherOfShop(1L)).thenReturn(List.of());

        assertEquals(List.of(), cacheService.getVoucherByShopId(1L));
        voucherListCache.invalidateAll();
        assertEquals(List.of(), cacheService.getVoucherByShopId(1L));
        assertEquals(List.of(), cacheService.getVoucherByShopId(1L));

        verify(voucherMapper, times(1)).queryVoucherOfShop(1L);
        verify(valueOperations, times(2)).get(key);
        ArgumentCaptor<Expiration> expiration = ArgumentCaptor.forClass(Expiration.class);
        verify(valueOperations).set(eq(key), eq("[]"), expiration.capture());
        assertEquals(CACHE_NULL_TTL * 60, expiration.getValue().getExpirationTimeInSeconds());
        assertNotNull(voucherListCache.getIfPresent(1L));
    }

    @Test
    void shouldBypassBothCachesAndQueryDatabaseEveryTimeInMysqlMode() {
        List<Voucher> cached = List.of(new Voucher().setId(10L).setTitle("旧优惠券"));
        List<Voucher> firstRead = List.of(new Voucher().setId(20L).setTitle("新优惠券"));
        List<Voucher> secondRead = List.of(new Voucher().setId(30L).setTitle("再次更新"));
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(CACHE_SHOP_VOUCHER_KEY + 1L)).thenReturn(JSONUtil.toJsonStr(cached));
        cacheService.getVoucherByShopId(1L);
        Object cachedEntry = voucherListCache.getIfPresent(1L);
        clearInvocations(stringRedisTemplate, valueOperations);
        cacheProperties.setMode(" MySQL ");
        when(voucherMapper.queryVoucherOfShop(1L)).thenReturn(firstRead, secondRead);

        assertEquals(firstRead, cacheService.getVoucherByShopId(1L));
        assertEquals(secondRead, cacheService.getVoucherByShopId(1L));

        verify(voucherMapper, times(2)).queryVoucherOfShop(1L);
        verifyNoInteractions(stringRedisTemplate);
        assertSame(cachedEntry, voucherListCache.getIfPresent(1L));
    }

    @Test
    void shouldBypassLocalCacheAndReadRedisEveryTimeInRedisMode() {
        List<Voucher> cached = List.of(new Voucher().setId(10L).setTitle("本地优惠券"));
        List<Voucher> vouchers = List.of(new Voucher().setId(20L).setTitle("Redis优惠券"));
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(CACHE_SHOP_VOUCHER_KEY + 1L)).thenReturn(JSONUtil.toJsonStr(cached), JSONUtil.toJsonStr(vouchers));
        cacheService.getVoucherByShopId(1L);
        Object cachedEntry = voucherListCache.getIfPresent(1L);
        clearInvocations(stringRedisTemplate, valueOperations);
        cacheProperties.setMode("REDIS");

        assertEquals(vouchers, cacheService.getVoucherByShopId(1L));
        assertEquals(vouchers, cacheService.getVoucherByShopId(1L));

        verify(valueOperations, times(2)).get(CACHE_SHOP_VOUCHER_KEY + 1L);
        verifyNoInteractions(voucherMapper);
        assertSame(cachedEntry, voucherListCache.getIfPresent(1L));
    }

    @Test
    void shouldFallBackToDatabaseAndOnlyPopulateRedisInRedisMode() {
        cacheProperties.setMode("redis");
        String key = CACHE_SHOP_VOUCHER_KEY + 1L;
        List<Voucher> vouchers = List.of(new Voucher().setId(10L).setShopId(1L));
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(key)).thenReturn(null, JSONUtil.toJsonStr(vouchers));
        when(voucherMapper.queryVoucherOfShop(1L)).thenReturn(vouchers);

        assertEquals(vouchers, cacheService.getVoucherByShopId(1L));
        assertEquals(vouchers, cacheService.getVoucherByShopId(1L));

        verify(valueOperations, times(2)).get(key);
        verify(voucherMapper, times(1)).queryVoucherOfShop(1L);
        ArgumentCaptor<Expiration> expiration = ArgumentCaptor.forClass(Expiration.class);
        verify(valueOperations).set(eq(key), eq(JSONUtil.toJsonStr(vouchers)), expiration.capture());
        assertEquals(cacheProperties.getRedisTtl().toSeconds(), expiration.getValue().getExpirationTimeInSeconds());
        assertNull(voucherListCache.getIfPresent(1L));
    }

    @Test
    void shouldNormalizeNullDatabaseResultToAnEmptyCachedList() {
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);

        assertEquals(List.of(), cacheService.getVoucherByShopId(1L));
        assertEquals(List.of(), cacheService.getVoucherByShopId(1L));

        verify(voucherMapper, times(1)).queryVoucherOfShop(1L);
        verify(valueOperations).set(eq(CACHE_SHOP_VOUCHER_KEY + 1L), eq("[]"), any(Expiration.class));
    }

    @Test
    void shouldServeOldLocalValueWhileRefreshingFromRedis() throws Exception {
        List<Voucher> oldVouchers = List.of(new Voucher().setId(10L).setTitle("旧优惠券"));
        List<Voucher> newVouchers = List.of(new Voucher().setId(20L).setTitle("新优惠券"));
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        CountDownLatch refreshStarted = new CountDownLatch(1);
        CountDownLatch allowRedisResult = new CountDownLatch(1);
        when(valueOperations.get(CACHE_SHOP_VOUCHER_KEY + 1L)).thenReturn(JSONUtil.toJsonStr(oldVouchers)).thenAnswer(invocation -> {
            refreshStarted.countDown();
            if (!allowRedisResult.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("等待Redis刷新返回超时");
            }
            return JSONUtil.toJsonStr(newVouchers);
        });

        cacheService.getVoucherByShopId(1L);
        try {
            CompletableFuture<?> refresh = voucherListCache.refresh(1L);
            assertTrue(refreshStarted.await(5, TimeUnit.SECONDS));
            assertEquals(oldVouchers, cacheService.getVoucherByShopId(1L));

            allowRedisResult.countDown();
            refresh.get(5, TimeUnit.SECONDS);
            // 刷新结果返回后，等待Caffeine完成异步写回
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(newVouchers, cacheService.getVoucherByShopId(1L)));
            verifyNoInteractions(voucherMapper);
        } finally {
            allowRedisResult.countDown();
        }
    }

    @Test
    void shouldLoadOneShopOnlyOnceForConcurrentLocalMisses() throws Exception {
        List<Voucher> vouchers = List.of(new Voucher().setId(10L).setShopId(1L));
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        CountDownLatch readersReady = new CountDownLatch(6);
        CountDownLatch startReads = new CountDownLatch(1);
        CountDownLatch databaseRead = new CountDownLatch(1);
        CountDownLatch allowDatabaseResult = new CountDownLatch(1);
        when(voucherMapper.queryVoucherOfShop(1L)).thenAnswer(invocation -> {
            databaseRead.countDown();
            if (!allowDatabaseResult.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("等待数据库返回超时");
            }
            return vouchers;
        });
        ExecutorService readers = Executors.newFixedThreadPool(6);
        try {
            List<Future<List<Voucher>>> results = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                results.add(readers.submit(() -> {
                    readersReady.countDown();
                    if (!startReads.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("等待并发查询开始超时");
                    }
                    return cacheService.getVoucherByShopId(1L);
                }));
            }
            assertTrue(readersReady.await(5, TimeUnit.SECONDS));
            startReads.countDown();
            assertTrue(databaseRead.await(5, TimeUnit.SECONDS));
            allowDatabaseResult.countDown();
            for (Future<List<Voucher>> result : results) {
                assertEquals(vouchers, result.get(5, TimeUnit.SECONDS));
            }

            verify(voucherMapper, times(1)).queryVoucherOfShop(1L);
            verify(valueOperations, times(1)).get(CACHE_SHOP_VOUCHER_KEY + 1L);
            verify(valueOperations, times(1)).set(eq(CACHE_SHOP_VOUCHER_KEY + 1L), anyString(), any(Expiration.class));
        } finally {
            startReads.countDown();
            allowDatabaseResult.countDown();
            readers.shutdownNow();
        }
    }
}
