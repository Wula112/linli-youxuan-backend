package com.hmdp.service.impl;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.common.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.framework.cache.RedisData;
import com.hmdp.cache.ShopBloomFilter;
import org.junit.jupiter.api.AfterEach;
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
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.LocalDateTime;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.hmdp.common.constant.RedisConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ShopServiceImplTest {

    @Mock
    private StringRedisTemplate stringRedisTemplate;
    @Mock
    private ValueOperations<String, String> valueOperations;
    @Mock
    private ShopBloomFilter shopBloomFilter;
    @Mock
    private ShopMapper shopMapper;
    @Mock
    private ScheduledExecutorService cacheDeleteExecutor;

    private ShopServiceImpl shopService;

    @BeforeEach
    void setUp() {
        shopService = spy(new ShopServiceImpl(stringRedisTemplate, shopBloomFilter, cacheDeleteExecutor));
        ReflectionTestUtils.setField(shopService, "baseMapper", shopMapper);
    }

    @AfterEach
    void tearDown() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void shouldRejectUnknownIdsWithoutQueryingDatabaseInAllCacheStrategies() {
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);

        assertFalse(shopService.queryById(999L).getSuccess());
        assertNull(shopService.queryWithPassThrough(999L));
        assertNull(shopService.queryWithMutex(999L));

        verifyNoInteractions(shopMapper);
        verify(valueOperations, never()).set(anyString(), anyString(), any(Expiration.class));
    }

    @Test
    void shouldCacheNullForBloomFalsePositiveAndAvoidRepeatedDatabaseQueries() {
        String key = CACHE_SHOP_KEY + 999L;
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(key)).thenReturn(null, "");
        when(shopBloomFilter.mightContain(999L)).thenReturn(true);
        doReturn(null).when(shopService).getById(999L);

        assertFalse(shopService.queryById(999L).getSuccess());
        assertFalse(shopService.queryById(999L).getSuccess());

        verify(shopService, times(1)).getById(999L);
        verify(shopBloomFilter, times(1)).mightContain(999L);
        ArgumentCaptor<Expiration> expiration = ArgumentCaptor.forClass(Expiration.class);
        verify(valueOperations).set(eq(key), eq(""), expiration.capture());
        assertEquals(CACHE_NULL_TTL * 60, expiration.getValue().getExpirationTimeInSeconds());
    }

    @Test
    void shouldQueryExistingShopOnColdCacheAndWriteLogicalExpiration() {
        String key = CACHE_SHOP_KEY + 1L;
        Shop shop = new Shop().setId(1L).setName("测试商铺");
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(shopBloomFilter.mightContain(1L)).thenReturn(true);
        doReturn(shop).when(shopService).getById(1L);

        Result result = shopService.queryById(1L);

        assertTrue(result.getSuccess());
        assertSame(shop, result.getData());
        ArgumentCaptor<String> json = ArgumentCaptor.forClass(String.class);
        verify(valueOperations).set(eq(key), json.capture());
        RedisData cached = JSONUtil.toBean(json.getValue(), RedisData.class);
        assertTrue(cached.getExpireTime().isAfter(LocalDateTime.now()));
        assertEquals(1L, ((JSONObject) cached.getData()).getLong("id"));
    }

    @Test
    void shouldReturnCachedShopWithoutCheckingBloomOrDatabase() {
        Shop shop = new Shop().setId(1L).setName("测试商铺");
        RedisData cached = new RedisData();
        cached.setData(shop);
        cached.setExpireTime(LocalDateTime.now().plusMinutes(10));
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(CACHE_SHOP_KEY + 1L)).thenReturn(JSONUtil.toJsonStr(cached));

        Result result = shopService.queryById(1L);

        assertTrue(result.getSuccess());
        assertEquals(1L, ((Shop) result.getData()).getId());
        verifyNoInteractions(shopBloomFilter, shopMapper);
    }

    @Test
    void shouldReadPlainShopCacheWrittenByOriginalPassThroughStrategy() {
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(CACHE_SHOP_KEY + 1L))
                .thenReturn(JSONUtil.toJsonStr(new Shop().setId(1L).setName("原有缓存")));

        Result result = shopService.queryById(1L);

        assertTrue(result.getSuccess());
        assertEquals("原有缓存", ((Shop) result.getData()).getName());
        verifyNoInteractions(shopBloomFilter, shopMapper);
    }

    @Test
    void shouldKeepPassThroughNullCacheAsEmptyString() {
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(shopBloomFilter.mightContain(999L)).thenReturn(true);
        doReturn(null).when(shopService).getById(999L);

        assertNull(shopService.queryWithPassThrough(999L));

        verify(valueOperations).set(eq(CACHE_SHOP_KEY + 999L), eq(""), any(Expiration.class));
    }

    @Test
    void shouldReadLegacyJsonNullCacheWithoutQueryingDatabase() {
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(CACHE_SHOP_KEY + 999L)).thenReturn("null");

        assertFalse(shopService.queryById(999L).getSuccess());
        assertNull(shopService.queryWithPassThrough(999L));
        assertNull(shopService.queryWithMutex(999L));

        verifyNoInteractions(shopBloomFilter, shopMapper);
    }

    @Test
    void shouldAddGeneratedShopIdToBloomBeforeCommitAndClearNullCacheAfterCommit() {
        Shop shop = new Shop().setName("新商铺");
        when(shopMapper.insert(shop)).thenAnswer(invocation -> {
            shop.setId(15L);
            return 1;
        });
        TransactionSynchronizationManager.initSynchronization();

        assertTrue(shopService.save(shop));

        verify(shopBloomFilter).add(15L);
        verify(stringRedisTemplate, never()).delete(anyString());
        for (TransactionSynchronization synchronization : TransactionSynchronizationManager.getSynchronizations()) {
            synchronization.afterCommit();
        }
        verify(stringRedisTemplate).delete(CACHE_SHOP_KEY + 15L);
        ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
        verify(cacheDeleteExecutor).schedule(task.capture(), eq(CACHE_SHOP_DELETE_DELAY), eq(TimeUnit.MILLISECONDS));
        task.getValue().run();
        verify(stringRedisTemplate, times(2)).delete(CACHE_SHOP_KEY + 15L);
    }

    @Test
    void shouldPropagateBloomWriteFailureSoShopInsertCanRollBack() {
        Shop shop = new Shop().setId(15L).setName("新商铺");
        when(shopMapper.insert(shop)).thenReturn(1);
        doThrow(new IllegalStateException("布隆过滤器写入失败")).when(shopBloomFilter).add(15L);
        TransactionSynchronizationManager.initSynchronization();

        assertThrows(IllegalStateException.class, () -> shopService.save(shop));

        assertTrue(TransactionSynchronizationManager.getSynchronizations().isEmpty());
        verifyNoInteractions(stringRedisTemplate);
    }

    @Test
    void shouldRejectInvalidIdsWithoutAccessingCacheOrDatabase() {
        assertFalse(shopService.queryById(null).getSuccess());
        assertFalse(shopService.queryById(0L).getSuccess());
        assertFalse(shopService.queryById(-1L).getSuccess());

        verifyNoInteractions(stringRedisTemplate, shopBloomFilter, shopMapper);
    }
}
