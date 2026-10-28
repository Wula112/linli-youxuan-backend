package com.hmdp.framework.cache;

import cn.hutool.json.JSONUtil;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.hmdp.framework.cache.config.LocalCacheProperties;
import com.hmdp.entity.ShopType;
import com.hmdp.entity.Voucher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LocalCacheClientTest {

    @Mock
    private StringRedisTemplate stringRedisTemplate;
    @Mock
    private ValueOperations<String, String> valueOperations;

    @BeforeEach
    void setUp() {
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @Test
    void shouldKeepSameLocalKeysAndDifferentElementTypesIsolatedBetweenInstances() {
        List<ShopType> types = List.of(new ShopType().setId(1L).setName("美食"));
        List<Voucher> vouchers = List.of(new Voucher().setId(10L).setTitle("代金券"));
        when(valueOperations.get("cache:types:same")).thenReturn(JSONUtil.toJsonStr(types));
        when(valueOperations.get("cache:vouchers:same")).thenReturn(JSONUtil.toJsonStr(vouchers));
        LocalCacheClient<String, ShopType> typeCache = new LocalCacheClient<>(stringRedisTemplate,
                new LocalCacheProperties(), ShopType.class, key -> "cache:types:" + key, key -> {
                    throw new AssertionError("Redis命中时不应该回源分类数据库");
                });
        LocalCacheClient<String, Voucher> voucherCache = new LocalCacheClient<>(stringRedisTemplate,
                new LocalCacheProperties(), Voucher.class, key -> "cache:vouchers:" + key, key -> {
                    throw new AssertionError("Redis命中时不应该回源优惠券数据库");
                });

        assertEquals(types, typeCache.get("same"));
        assertEquals(vouchers, voucherCache.get("same"));
        assertEquals(types, typeCache.get("same"));
        assertEquals(vouchers, voucherCache.get("same"));

        verify(valueOperations, times(1)).get("cache:types:same");
        verify(valueOperations, times(1)).get("cache:vouchers:same");
    }

    @Test
    void shouldKeepOldLocalValueWhenAsynchronousRefreshFails() throws Exception {
        List<Voucher> vouchers = List.of(new Voucher().setId(10L).setTitle("代金券"));
        when(valueOperations.get("cache:vouchers:1")).thenReturn(JSONUtil.toJsonStr(vouchers))
                .thenThrow(new IllegalStateException("Redis不可用"));
        LocalCacheClient<Long, Voucher> cacheClient = new LocalCacheClient<>(stringRedisTemplate,
                new LocalCacheProperties(), Voucher.class, id -> "cache:vouchers:" + id, id -> {
                    throw new AssertionError("不应该绕过Redis异常直接访问数据库");
                });
        assertEquals(vouchers, cacheClient.get(1L));
        LoadingCache<Long, ?> localCache = (LoadingCache<Long, ?>)
                ReflectionTestUtils.getField(cacheClient, "localCache");

        ExecutionException exception = assertThrows(ExecutionException.class,
                () -> localCache.refresh(1L).get(5, TimeUnit.SECONDS));

        assertInstanceOf(IllegalStateException.class, exception.getCause());
        assertEquals(vouchers, cacheClient.get(1L));
        verify(valueOperations, times(2)).get("cache:vouchers:1");
    }
}
