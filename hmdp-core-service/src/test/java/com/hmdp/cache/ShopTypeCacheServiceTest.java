package com.hmdp.cache;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.hmdp.cache.config.ShopTypeCacheProperties;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
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

import java.util.List;

import static com.hmdp.common.constant.RedisConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ShopTypeCacheServiceTest {

    @Mock
    private StringRedisTemplate stringRedisTemplate;
    @Mock
    private ValueOperations<String, String> valueOperations;
    @Mock
    private ShopTypeMapper shopTypeMapper;

    private LoadingCache<String, ?> shopTypeCache;
    private ShopTypeCacheProperties cacheProperties;
    private ShopTypeCacheService cacheService;

    @BeforeEach
    void setUp() {
        cacheProperties = new ShopTypeCacheProperties();
        cacheService = new ShopTypeCacheService();
        ReflectionTestUtils.setField(cacheService, "shopTypeMapper", shopTypeMapper);
        ReflectionTestUtils.setField(cacheService, "stringRedisTemplate", stringRedisTemplate);
        ReflectionTestUtils.setField(cacheService, "cacheProperties", cacheProperties);
        cacheService.initCache();
        Object cacheClient = ReflectionTestUtils.getField(cacheService, "shopTypeCache");
        shopTypeCache = (LoadingCache<String, ?>)
                ReflectionTestUtils.getField(cacheClient, "localCache");
    }

    @Test
    void shouldReturnLocalCacheWithoutAccessingRedisOrDatabase() {
        List<ShopType> types = List.of(new ShopType().setId(1L).setName("美食"));
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(CACHE_SHOP_TYPE_KEY)).thenReturn(JSONUtil.toJsonStr(types));
        cacheService.getTypeList();
        clearInvocations(stringRedisTemplate, valueOperations);

        assertEquals(types, cacheService.getTypeList());
        verifyNoInteractions(stringRedisTemplate, shopTypeMapper);
    }

    @Test
    void shouldPopulateLocalCacheOnRedisHit() {
        List<ShopType> types = List.of(new ShopType().setId(1L).setName("美食").setSort(1));
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(CACHE_SHOP_TYPE_KEY)).thenReturn(JSONUtil.toJsonStr(types));

        assertEquals(types, cacheService.getTypeList());
        assertEquals(types, cacheService.getTypeList());

        assertNotNull(shopTypeCache.getIfPresent(CACHE_SHOP_TYPE_KEY));
        verify(valueOperations, times(1)).get(CACHE_SHOP_TYPE_KEY);
        verifyNoInteractions(shopTypeMapper);
        verify(valueOperations, never()).set(anyString(), anyString(), any(Expiration.class));
    }

    @Test
    void shouldQuerySortedTypesAndPopulateBothCachesOnMiss() {
        List<ShopType> types = List.of(
                new ShopType().setId(2L).setName("美食").setSort(1),
                new ShopType().setId(1L).setName("健身").setSort(2));
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(shopTypeMapper.selectList(any(Wrapper.class))).thenReturn(types);

        assertEquals(types, cacheService.getTypeList());
        assertEquals(types, cacheService.getTypeList());

        ArgumentCaptor<Wrapper<ShopType>> query = ArgumentCaptor.forClass(Wrapper.class);
        verify(shopTypeMapper, times(1)).selectList(query.capture());
        assertTrue(query.getValue().getSqlSegment().contains("ORDER BY sort ASC"));
        ArgumentCaptor<Expiration> expiration = ArgumentCaptor.forClass(Expiration.class);
        verify(valueOperations).set(eq(CACHE_SHOP_TYPE_KEY), eq(JSONUtil.toJsonStr(types)), expiration.capture());
        assertEquals(cacheProperties.getRedisTtl().toSeconds(), expiration.getValue().getExpirationTimeInSeconds());
        assertNotNull(shopTypeCache.getIfPresent(CACHE_SHOP_TYPE_KEY));
        verify(valueOperations, times(1)).get(CACHE_SHOP_TYPE_KEY);
    }

    @Test
    void shouldCacheEmptyTypeListToAvoidRepeatedDatabaseQueries() {
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(shopTypeMapper.selectList(any(Wrapper.class))).thenReturn(List.of());

        assertEquals(List.of(), cacheService.getTypeList());
        assertEquals(List.of(), cacheService.getTypeList());

        assertNotNull(shopTypeCache.getIfPresent(CACHE_SHOP_TYPE_KEY));
        verify(shopTypeMapper, times(1)).selectList(any(Wrapper.class));
        ArgumentCaptor<Expiration> expiration = ArgumentCaptor.forClass(Expiration.class);
        verify(valueOperations).set(eq(CACHE_SHOP_TYPE_KEY), eq("[]"), expiration.capture());
        assertEquals(CACHE_NULL_TTL * 60, expiration.getValue().getExpirationTimeInSeconds());
    }

    @Test
    void shouldReadRedisAgainAfterLocalEntryIsEvicted() {
        List<ShopType> types = List.of(new ShopType().setId(1L).setName("美食"));
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(CACHE_SHOP_TYPE_KEY)).thenReturn(JSONUtil.toJsonStr(types));
        cacheService.getTypeList();
        shopTypeCache.invalidateAll();

        assertEquals(types, cacheService.getTypeList());

        verify(valueOperations, times(2)).get(CACHE_SHOP_TYPE_KEY);
        verifyNoInteractions(shopTypeMapper);
    }

    @Test
    void shouldQuerySortedTypesEveryTimeAndBypassBothCachesInMysqlMode() {
        List<ShopType> cached = List.of(new ShopType().setId(1L).setName("旧分类"));
        List<ShopType> firstRead = List.of(new ShopType().setId(2L).setName("新分类"));
        List<ShopType> secondRead = List.of(new ShopType().setId(3L).setName("再次更新"));
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(CACHE_SHOP_TYPE_KEY)).thenReturn(JSONUtil.toJsonStr(cached));
        cacheService.getTypeList();
        Object cachedEntry = shopTypeCache.getIfPresent(CACHE_SHOP_TYPE_KEY);
        clearInvocations(stringRedisTemplate, valueOperations);
        cacheProperties.setMode("MySQL");
        when(shopTypeMapper.selectList(any(Wrapper.class))).thenReturn(firstRead, secondRead);

        assertEquals(firstRead, cacheService.getTypeList());
        assertEquals(secondRead, cacheService.getTypeList());

        verifyNoInteractions(stringRedisTemplate);
        ArgumentCaptor<Wrapper<ShopType>> queries = ArgumentCaptor.forClass(Wrapper.class);
        verify(shopTypeMapper, times(2)).selectList(queries.capture());
        assertTrue(queries.getAllValues().stream().allMatch(query -> query.getSqlSegment().contains("ORDER BY sort ASC")));
        assertSame(cachedEntry, shopTypeCache.getIfPresent(CACHE_SHOP_TYPE_KEY));
    }

    @Test
    void shouldBypassLocalCacheAndReadRedisEveryTimeInRedisMode() {
        List<ShopType> cached = List.of(new ShopType().setId(1L).setName("旧分类"));
        List<ShopType> types = List.of(new ShopType().setId(2L).setName("新分类"));
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(CACHE_SHOP_TYPE_KEY)).thenReturn(JSONUtil.toJsonStr(cached), JSONUtil.toJsonStr(types));
        cacheService.getTypeList();
        Object cachedEntry = shopTypeCache.getIfPresent(CACHE_SHOP_TYPE_KEY);
        clearInvocations(stringRedisTemplate, valueOperations);
        cacheProperties.setMode("REDIS");

        assertEquals(types, cacheService.getTypeList());
        assertEquals(types, cacheService.getTypeList());

        verify(valueOperations, times(2)).get(CACHE_SHOP_TYPE_KEY);
        verifyNoInteractions(shopTypeMapper);
        assertSame(cachedEntry, shopTypeCache.getIfPresent(CACHE_SHOP_TYPE_KEY));
    }

    @Test
    void shouldOnlyPopulateRedisAfterDatabaseFallbackInRedisMode() {
        cacheProperties.setMode("redis");
        List<ShopType> types = List.of(new ShopType().setId(1L).setName("美食").setSort(1));
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(CACHE_SHOP_TYPE_KEY)).thenReturn(null, JSONUtil.toJsonStr(types));
        when(shopTypeMapper.selectList(any(Wrapper.class))).thenReturn(types);

        assertEquals(types, cacheService.getTypeList());
        assertEquals(types, cacheService.getTypeList());

        verify(shopTypeMapper, times(1)).selectList(any(Wrapper.class));
        verify(valueOperations, times(2)).get(CACHE_SHOP_TYPE_KEY);
        verify(valueOperations).set(eq(CACHE_SHOP_TYPE_KEY), eq(JSONUtil.toJsonStr(types)), any(Expiration.class));
        assertNull(shopTypeCache.getIfPresent(CACHE_SHOP_TYPE_KEY));
    }
}
