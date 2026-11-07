package com.hmdp.service.impl;

import com.hmdp.entity.VoucherOrderProcess;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mapper.VoucherOrderProcessMapper;
import com.hmdp.repository.redis.SeckillReservation;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class VoucherOrderCompensationServiceTest {
    private final VoucherOrderProcessMapper mapper = mock(VoucherOrderProcessMapper.class);
    private final VoucherOrderProcessService processes = mock(VoucherOrderProcessService.class);
    private final SeckillReservation reservation = mock(SeckillReservation.class);
    private final VoucherOrderMapper orders = mock(VoucherOrderMapper.class);
    private final VoucherOrderCompensationService service =
            new VoucherOrderCompensationService(mapper, processes, reservation, orders);

    @Test
    void confirmedFirstReleaseOrRetryCompletesTask() {
        when(mapper.selectById(1000L)).thenReturn(new VoucherOrderProcess()
                .setOrderId(1000L).setUserId(5L).setVoucherId(10L).setStatus("COMPENSATING"));
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenReturn(new VoucherOrder().setStatus(4));
        when(reservation.rollback(10L, 5L, 1000L)).thenReturn(1L, 2L);
        service.compensate(1000L);
        service.compensate(1000L);
        verify(processes, times(2)).markCompensated(1000L);
    }

    @Test
    void missingReservationWithoutReceiptDoesNotCompleteTask() {
        when(mapper.selectById(1000L)).thenReturn(new VoucherOrderProcess()
                .setOrderId(1000L).setUserId(5L).setVoucherId(10L).setStatus("COMPENSATING"));
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenReturn(new VoucherOrder().setStatus(4));
        when(reservation.rollback(10L, 5L, 1000L)).thenReturn(0L);
        assertThrows(IllegalStateException.class, () -> service.compensate(1000L));
        verifyNoInteractions(processes);
    }

    @Test
    void neverReleasesStockForMissingOrActiveOrder() {
        when(mapper.selectById(1000L)).thenReturn(new VoucherOrderProcess()
                .setOrderId(1000L).setUserId(5L).setVoucherId(10L).setStatus("COMPENSATING"));
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenReturn(null, new VoucherOrder().setStatus(1));
        assertThrows(IllegalStateException.class, () -> service.compensate(1000L));
        assertThrows(IllegalStateException.class, () -> service.compensate(1000L));
        verifyNoInteractions(reservation, processes);
    }
}
