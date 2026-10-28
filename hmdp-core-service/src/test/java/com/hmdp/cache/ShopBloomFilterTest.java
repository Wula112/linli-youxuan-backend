package com.hmdp.cache;

import com.hmdp.common.constant.RedisConstants;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.codec.StringCodec;

import java.util.Collections;
import java.util.List;

import static com.hmdp.common.constant.RedisConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ShopBloomFilterTest {

    @Mock
    private ShopMapper shopMapper;
    @Mock
    private RedissonClient redissonClient;
    @Mock
    private RBloomFilter<Long> bloomFilter;
    @Mock
    private RBucket<String> bloomData;
    @Mock
    private RBucket<String> ready;

    private ShopBloomFilter shopBloomFilter;

    @BeforeEach
    void setUp() {
        when(redissonClient.<Long>getBloomFilter(BLOOM_SHOP_KEY, LongCodec.INSTANCE)).thenReturn(bloomFilter);
        when(redissonClient.<String>getBucket(BLOOM_SHOP_KEY, StringCodec.INSTANCE)).thenReturn(bloomData);
        when(redissonClient.<String>getBucket(BLOOM_SHOP_READY_KEY, StringCodec.INSTANCE)).thenReturn(ready);
        shopBloomFilter = new ShopBloomFilter(shopMapper, redissonClient);
    }

    @Test
    void shouldLoadAllBatchesBeforeEnablingFilter() {
        when(shopMapper.selectList(any(Wrapper.class))).thenReturn(
                List.of(new Shop().setId(1L), new Shop().setId(5L)),
                List.of(new Shop().setId(9L)), Collections.emptyList());

        shopBloomFilter.run(null);

        InOrder order = inOrder(ready, bloomFilter, shopMapper);
        order.verify(ready).delete();
        order.verify(bloomFilter).tryInit(BLOOM_SHOP_EXPECTED_INSERTIONS, BLOOM_SHOP_FALSE_PROBABILITY);
        order.verify(bloomFilter).add(0L);
        order.verify(shopMapper).selectList(any(Wrapper.class));
        order.verify(bloomFilter).add(List.of(1L, 5L));
        order.verify(shopMapper).selectList(any(Wrapper.class));
        order.verify(bloomFilter).add(List.of(9L));
        order.verify(shopMapper).selectList(any(Wrapper.class));
        order.verify(ready).set(anyString());
        verify(bloomFilter, never()).delete();
    }

    @Test
    void shouldKeepOriginalQueryAvailableWhenWarmupFails() {
        when(shopMapper.selectList(any(Wrapper.class)))
                .thenReturn(List.of(new Shop().setId(1L)))
                .thenThrow(new IllegalStateException("数据库不可用"));

        assertDoesNotThrow(() -> shopBloomFilter.run(null));

        verify(ready, never()).set(any());
        assertTrue(shopBloomFilter.mightContain(2L));
        verify(bloomFilter, never()).contains(any(Long.class));
    }

    @Test
    void shouldAllowQueriesBeforeWarmupCompletes() {
        assertTrue(shopBloomFilter.mightContain(1L));

        verify(bloomFilter, never()).contains(any(Long.class));
    }

    @Test
    void shouldRejectUnknownIdsAfterWarmup() {
        when(ready.get()).thenReturn("1");
        when(bloomData.isExists()).thenReturn(true);
        when(bloomFilter.contains(999L)).thenReturn(false);

        assertFalse(shopBloomFilter.mightContain(999L));
    }

    @Test
    void shouldAllowPossibleMatches() {
        when(ready.get()).thenReturn("1");
        when(bloomFilter.contains(1L)).thenReturn(true);

        assertTrue(shopBloomFilter.mightContain(1L));
    }

    @Test
    void shouldAllowQueriesWhenFilterIsMissing() {
        when(ready.get()).thenReturn("1");
        when(bloomData.isExists()).thenReturn(false);

        assertTrue(shopBloomFilter.mightContain(1L));
        verify(ready).delete();
    }

    @Test
    void shouldAllowQueriesWhenOnlyBloomConfigurationIsMissing() {
        when(ready.get()).thenReturn("1");
        when(bloomFilter.getSize()).thenThrow(new IllegalStateException("过滤器配置丢失"));

        assertTrue(shopBloomFilter.mightContain(1L));
    }

    @Test
    void shouldAllowQueriesWhenFilterIsRebuiltDuringMembershipCheck() {
        when(ready.get()).thenReturn("旧版本", "新版本");
        when(bloomData.isExists()).thenReturn(true);

        assertTrue(shopBloomFilter.mightContain(1L));
    }

    @Test
    void shouldAllowQueriesWhenBloomCheckFails() {
        when(ready.get()).thenReturn("1");
        when(bloomFilter.contains(1L)).thenThrow(new IllegalStateException("Redis不可用"));

        assertTrue(shopBloomFilter.mightContain(1L));
    }

    @Test
    void shouldNotEnableFilteringAfterAddingOnlyOneIdToNewFilter() {
        when(bloomFilter.tryInit(BLOOM_SHOP_EXPECTED_INSERTIONS, BLOOM_SHOP_FALSE_PROBABILITY)).thenReturn(true);
        when(bloomData.isExists()).thenReturn(true);

        shopBloomFilter.add(1L);

        InOrder order = inOrder(bloomFilter, ready);
        order.verify(bloomFilter).tryInit(BLOOM_SHOP_EXPECTED_INSERTIONS, BLOOM_SHOP_FALSE_PROBABILITY);
        order.verify(ready).delete();
        order.verify(bloomFilter).add(1L);
        verify(ready, never()).set(any());
    }

    @Test
    void shouldDisableFilteringWhenAddingToFilterWithMissingBitmap() {
        when(bloomData.isExists()).thenReturn(false);

        shopBloomFilter.add(15L);

        InOrder order = inOrder(ready, bloomFilter);
        order.verify(ready).delete();
        order.verify(bloomFilter).add(15L);
        verify(ready, never()).set(any());
    }

    @Test
    void shouldRejectInvalidIdsWithoutAccessingRedis() {
        assertFalse(shopBloomFilter.mightContain(null));
        assertFalse(shopBloomFilter.mightContain(0L));
        assertFalse(shopBloomFilter.mightContain(-1L));

        verifyNoInteractions(bloomFilter, bloomData, ready);
    }
}
