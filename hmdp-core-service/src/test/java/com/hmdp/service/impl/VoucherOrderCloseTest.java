package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.conditions.update.UpdateChainWrapper;
import com.hmdp.common.enums.OrderProcessStatus;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.entity.VoucherOrderProcess;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.SimpleTransactionStatus;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class VoucherOrderCloseTest {
    private final VoucherOrderCloseService service = new VoucherOrderCloseService();
    private final VoucherOrderMapper orders = mock(VoucherOrderMapper.class);
    private final SeckillVoucherMapper stocks = mock(SeckillVoucherMapper.class);
    private final ISeckillVoucherService seckill = mock(ISeckillVoucherService.class);
    private final VoucherOrderProcessService processes = mock(VoucherOrderProcessService.class);
    private final PlatformTransactionManager manager = mock(PlatformTransactionManager.class);
    private final VoucherOrderProcess process = new VoucherOrderProcess()
            .setOrderId(1000L).setUserId(5L).setVoucherId(10L).setStatus("CREATED");

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(service, "voucherOrderMapper", orders);
        com.hmdp.support.OrderRoutingTestSupport.install(service);
        ReflectionTestUtils.setField(service, "seckillVoucherService", seckill);
        ReflectionTestUtils.setField(service, "processService", processes);
        ReflectionTestUtils.setField(service, "transactionManager", manager);
        when(manager.getTransaction(any())).thenAnswer(i -> new SimpleTransactionStatus());
        when(processes.lock(1000L, 5L, 10L)).thenReturn(process);
        doAnswer(i -> { process.setStatus(((OrderProcessStatus) i.getArgument(1)).name()); return null; })
                .when(processes).transition(any(), any());
        when(seckill.update()).thenReturn(new UpdateChainWrapper<>(stocks));
        when(stocks.update(isNull(), any())).thenReturn(1);
    }

    @Test
    void shouldRecordCompensationInSameTransactionAsCloseAndDatabaseRestore() {
        when(orders.closeIfExpired(1000L, 5L, 10L)).thenReturn(1);
        service.closeExpiredOrder(1000L);
        var sequence = inOrder(processes, orders, stocks, manager);
        sequence.verify(processes).lock(1000L, 5L, 10L);
        sequence.verify(orders).closeIfExpired(1000L, 5L, 10L);
        sequence.verify(stocks).update(isNull(), any());
        sequence.verify(processes).transition(process, OrderProcessStatus.COMPENSATING);
        sequence.verify(manager).commit(any());
    }

    @Test
    void shouldNotRestoreDatabaseStockTwice() {
        when(orders.closeIfExpired(1000L, 5L, 10L)).thenReturn(1);
        service.closeExpiredOrder(1000L);
        service.closeExpiredOrder(1000L);
        verify(stocks, times(1)).update(isNull(), any());
        verify(processes, times(1)).transition(process, OrderProcessStatus.COMPENSATING);
    }

    @Test
    void shouldRollbackCloseWhenDatabaseStockCannotBeRestored() {
        when(orders.closeIfExpired(1000L, 5L, 10L)).thenReturn(1);
        when(stocks.update(isNull(), any())).thenReturn(0);
        assertThrows(IllegalStateException.class, () -> service.closeExpiredOrder(1000L));
        verify(manager).rollback(any());
        verify(processes, never()).transition(any(), eq(OrderProcessStatus.COMPENSATING));
    }

    @Test
    void shouldNotCompensatePaidOrNotYetExpiredOrder() {
        when(orders.closeIfExpired(1000L, 5L, 10L)).thenReturn(0);
        service.closeExpiredOrder(1000L);
        verifyNoInteractions(stocks);
        verify(processes, never()).transition(any(), eq(OrderProcessStatus.COMPENSATING));
    }

    @Test
    void shouldRecognizeHistoricalActiveOrderBeforeTryingToCloseIt() {
        process.setStatus("PENDING");
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenReturn(new VoucherOrder().setStatus(1));
        service.closeExpiredOrder(1000L);
        verify(processes).transition(process, OrderProcessStatus.CREATED);
    }
}
