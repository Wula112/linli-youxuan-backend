package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.common.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.cache.ShopBloomFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hmdp.common.constant.RedisConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ShopCacheDeleteTest {

    @Mock
    private StringRedisTemplate stringRedisTemplate;
    @Mock
    private ValueOperations<String, String> valueOperations;
    @Mock
    private ShopBloomFilter shopBloomFilter;
    @Mock
    private ScheduledExecutorService cacheDeleteExecutor;

    private ShopServiceImpl shopService;
    private TransactionTemplate transactionTemplate;

    @BeforeEach
    void setUp() {
        shopService = spy(new ShopServiceImpl(stringRedisTemplate, shopBloomFilter, cacheDeleteExecutor));
        transactionTemplate = new TransactionTemplate(new TestTransactionManager());
    }

    @Test
    void shouldDeleteAfterCommitAndScheduleSecondDelete() {
        Shop shop = new Shop().setId(1L).setName("更新后的商铺");
        doReturn(true).when(shopService).updateById(shop);

        transactionTemplate.executeWithoutResult(status -> {
            assertTrue(shopService.update(shop).getSuccess());
            verifyNoInteractions(stringRedisTemplate, cacheDeleteExecutor);
        });

        ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
        InOrder order = inOrder(stringRedisTemplate, cacheDeleteExecutor);
        order.verify(stringRedisTemplate).delete(CACHE_SHOP_KEY + 1L);
        order.verify(cacheDeleteExecutor).schedule(task.capture(), eq(CACHE_SHOP_DELETE_DELAY), eq(TimeUnit.MILLISECONDS));
        task.getValue().run();
        verify(stringRedisTemplate, times(2)).delete(CACHE_SHOP_KEY + 1L);
    }

    @Test
    void shouldNotDeleteOrScheduleWhenTransactionRollsBack() {
        Shop shop = new Shop().setId(1L);
        doReturn(true).when(shopService).updateById(shop);

        transactionTemplate.executeWithoutResult(status -> {
            assertTrue(shopService.update(shop).getSuccess());
            status.setRollbackOnly();
        });

        verifyNoInteractions(stringRedisTemplate, cacheDeleteExecutor);
    }

    @Test
    void shouldNotDeleteOrScheduleWhenUpdateFails() {
        Shop shop = new Shop().setId(999L);
        doReturn(false).when(shopService).updateById(shop);

        Result result = transactionTemplate.execute(status -> shopService.update(shop));

        assertNotNull(result);
        assertFalse(result.getSuccess());
        verifyNoInteractions(stringRedisTemplate, cacheDeleteExecutor);
    }

    @Test
    void shouldRejectMissingShopIdWithoutDeletingCache() {
        assertFalse(shopService.update(new Shop()).getSuccess());

        verify(shopService, never()).updateById(any(Shop.class));
        verifyNoInteractions(stringRedisTemplate, cacheDeleteExecutor);
    }

    @Test
    void shouldStillScheduleSecondDeleteWhenFirstDeleteFails() {
        Shop shop = new Shop().setId(1L);
        doReturn(true).when(shopService).updateById(shop);
        when(stringRedisTemplate.delete(CACHE_SHOP_KEY + 1L))
                .thenThrow(new IllegalStateException("第一次删除失败")).thenReturn(true);

        Result result = transactionTemplate.execute(status -> shopService.update(shop));

        assertNotNull(result);
        assertTrue(result.getSuccess());
        ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
        verify(cacheDeleteExecutor).schedule(task.capture(), eq(CACHE_SHOP_DELETE_DELAY), eq(TimeUnit.MILLISECONDS));
        assertDoesNotThrow(() -> task.getValue().run());
        verify(stringRedisTemplate, times(2)).delete(CACHE_SHOP_KEY + 1L);
    }

    @Test
    void shouldNotReportCommittedUpdateAsFailureWhenSchedulingIsRejected() {
        Shop shop = new Shop().setId(1L);
        doReturn(true).when(shopService).updateById(shop);
        doThrow(new RejectedExecutionException("线程池关闭"))
                .when(cacheDeleteExecutor).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

        Result result = transactionTemplate.execute(status -> shopService.update(shop));

        assertNotNull(result);
        assertTrue(result.getSuccess());
        verify(stringRedisTemplate).delete(CACHE_SHOP_KEY + 1L);
    }

    @Test
    void shouldHandleDelayedDeleteFailureInsideScheduledTask() {
        Shop shop = new Shop().setId(1L);
        doReturn(true).when(shopService).updateById(shop);
        when(stringRedisTemplate.delete(CACHE_SHOP_KEY + 1L))
                .thenReturn(true).thenThrow(new IllegalStateException("延迟删除失败"));
        transactionTemplate.executeWithoutResult(status -> shopService.update(shop));
        ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
        verify(cacheDeleteExecutor).schedule(task.capture(), eq(CACHE_SHOP_DELETE_DELAY), eq(TimeUnit.MILLISECONDS));

        assertDoesNotThrow(() -> task.getValue().run());
        verify(stringRedisTemplate, times(2)).delete(CACHE_SHOP_KEY + 1L);
    }

    @Test
    void shouldClearOldValueWrittenByConcurrentColdQueryAfterCommit() throws Exception {
        String key = CACHE_SHOP_KEY + 1L;
        Shop oldShop = new Shop().setId(1L).setName("旧商铺名称");
        Shop newShop = new Shop().setId(1L).setName("新商铺名称");
        AtomicReference<Shop> database = new AtomicReference<>(oldShop);
        Map<String, String> cache = new ConcurrentHashMap<>();
        CountDownLatch oldValueRead = new CountDownLatch(1);
        CountDownLatch allowCacheWrite = new CountDownLatch(1);
        when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(key)).thenAnswer(invocation -> cache.get(key));
        doAnswer(invocation -> {
            cache.put(key, invocation.getArgument(1));
            return null;
        }).when(valueOperations).set(eq(key), anyString());
        when(stringRedisTemplate.delete(key)).thenAnswer(invocation -> cache.remove(key) != null);
        when(shopBloomFilter.mightContain(1L)).thenReturn(true);
        doAnswer(invocation -> {
            Shop snapshot = database.get();
            oldValueRead.countDown();
            if (!allowCacheWrite.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("等待更新事务提交超时");
            }
            return snapshot;
        }).when(shopService).getById(1L);
        doAnswer(invocation -> {
            database.set(newShop);
            return true;
        }).when(shopService).updateById(newShop);

        ExecutorService reader = Executors.newSingleThreadExecutor();
        try {
            Future<Result> readResult = reader.submit(() -> shopService.queryById(1L));
            assertTrue(oldValueRead.await(5, TimeUnit.SECONDS));

            Result updateResult = transactionTemplate.execute(status -> shopService.update(newShop));
            assertNotNull(updateResult);
            assertTrue(updateResult.getSuccess());
            ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
            verify(cacheDeleteExecutor).schedule(task.capture(), eq(CACHE_SHOP_DELETE_DELAY), eq(TimeUnit.MILLISECONDS));

            // 第一次删除后，读线程才把更新前查到的旧值写入缓存
            allowCacheWrite.countDown();
            assertTrue(readResult.get(5, TimeUnit.SECONDS).getSuccess());
            assertEquals("旧商铺名称", JSONUtil.parseObj(cache.get(key)).getJSONObject("data").getStr("name"));
            assertSame(newShop, database.get());

            // 执行已经捕获的延迟任务，清理刚刚回写的旧缓存
            task.getValue().run();
            assertNull(cache.get(key));
            verify(stringRedisTemplate, times(2)).delete(key);
        } finally {
            allowCacheWrite.countDown();
            reader.shutdownNow();
        }
    }

    // 使用Spring真实的事务回调流程，不连接数据库
    private static class TestTransactionManager extends AbstractPlatformTransactionManager {
        @Override
        protected Object doGetTransaction() {
            return new Object();
        }

        @Override
        protected void doBegin(Object transaction, TransactionDefinition definition) {
        }

        @Override
        protected void doCommit(DefaultTransactionStatus status) {
        }

        @Override
        protected void doRollback(DefaultTransactionStatus status) {
        }
    }
}
