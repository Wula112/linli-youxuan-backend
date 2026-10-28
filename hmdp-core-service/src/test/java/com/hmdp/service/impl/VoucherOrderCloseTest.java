package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.conditions.update.UpdateChainWrapper;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class VoucherOrderCloseTest {

    private final VoucherOrderServiceImpl service = new VoucherOrderServiceImpl();
    private final VoucherOrderMapper orders = mock(VoucherOrderMapper.class);
    private final SeckillVoucherMapper stocks = mock(SeckillVoucherMapper.class);
    private final ISeckillVoucherService seckill = mock(ISeckillVoucherService.class);
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final PlatformTransactionManager manager = mock(PlatformTransactionManager.class);
    private final VoucherOrder closed = new VoucherOrder().setId(1000L).setUserId(5L).setVoucherId(10L)
            .setStatus(4).setRedisStockRestored(false);

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(service, "baseMapper", orders);
        com.hmdp.support.OrderRoutingTestSupport.install(service);
        ReflectionTestUtils.setField(service, "seckillVoucherService", seckill);
        ReflectionTestUtils.setField(service, "stringRedisTemplate", redis);
        ReflectionTestUtils.setField(service, "transactionManager", manager);
        ReflectionTestUtils.setField(service, "voucherMapper", mock(VoucherMapper.class));
        ReflectionTestUtils.setField(service, "eventPublisher", mock(ApplicationEventPublisher.class));
        when(manager.getTransaction(any())).thenAnswer(invocation -> new SimpleTransactionStatus());
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenReturn(closed);
        when(seckill.update()).thenReturn(new UpdateChainWrapper<>(stocks));
        when(stocks.update(isNull(), any())).thenReturn(1);
        when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString())).thenReturn(1L);
    }

    @Test
    void shouldCommitCloseAndDatabaseStockBeforeReleasingRedisQualification() {
        when(orders.closeIfExpired(1000L, 5L, 10L)).thenReturn(1);
        service.closeExpiredOrder(1000L);
        var sequence = inOrder(orders, stocks, manager, redis);
        sequence.verify(orders).closeIfExpired(1000L, 5L, 10L);
        sequence.verify(orders).selectRoutedOrder(1000L, 5L, 10L);
        sequence.verify(stocks).update(isNull(), any());
        sequence.verify(manager).commit(any());
        sequence.verify(redis).execute(any(RedisScript.class),
                eq(List.of("seckill:stock:10", "seckill:reservation:10", "seckill:pending", "seckill:pending:retries")),
                eq("5"), eq("1000"), eq("10:5:1000"));
        sequence.verify(orders).markRedisStockRestored(1000L, 5L, 10L);
        sequence.verify(manager).commit(any());
    }

    @Test
    void shouldRetryRedisRestoreWithoutRestoringDatabaseStockTwice() {
        when(orders.closeIfExpired(1000L, 5L, 10L)).thenReturn(1, 0);
        when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString()))
                .thenThrow(new IllegalStateException("lost Redis reply")).thenReturn(0L);
        assertThrows(IllegalStateException.class, () -> service.closeExpiredOrder(1000L));
        service.closeExpiredOrder(1000L);
        verify(stocks, times(1)).update(isNull(), any());
        verify(orders, times(1)).markRedisStockRestored(1000L, 5L, 10L);
    }

    @Test
    void shouldRollbackCloseWhenDatabaseStockCannotBeRestored() {
        when(orders.closeIfExpired(1000L, 5L, 10L)).thenReturn(1);
        when(stocks.update(isNull(), any())).thenReturn(0);
        assertThrows(IllegalStateException.class, () -> service.closeExpiredOrder(1000L));
        verify(manager).rollback(any());
        verifyNoInteractions(redis);
        verify(orders, never()).markRedisStockRestored(anyLong(), anyLong(), anyLong());
    }

    @Test
    void shouldNotRestorePaidOrAlreadyRestoredOrder() {
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenReturn(closed.setStatus(2));
        service.closeExpiredOrder(1000L);
        closed.setStatus(4).setRedisStockRestored(true);
        service.closeExpiredOrder(1000L);
        verifyNoInteractions(redis, stocks);
    }
}
