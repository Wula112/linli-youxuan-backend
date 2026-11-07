package com.hmdp.service.impl;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.entity.VoucherOrderProcess;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mapper.VoucherOrderProcessMapper;
import com.hmdp.mq.message.VoucherOrderMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class VoucherOrderProcessServiceTest {
    private final VoucherOrderProcessMapper mapper = mock(VoucherOrderProcessMapper.class);
    private final VoucherOrderMapper orders = mock(VoucherOrderMapper.class);
    private final VoucherOrderProcessService service = new VoucherOrderProcessService(mapper, orders);
    private final VoucherOrderProcess row = new VoucherOrderProcess().setOrderId(1000L).setUserId(5L)
            .setVoucherId(10L).setStatus("PENDING");
    private final VoucherOrderMessage event = new VoucherOrderMessage(1000L, 5L, 10L);

    @BeforeEach
    void setUp() {
        when(mapper.selectForUpdate(1000L)).thenReturn(row);
        when(mapper.transition(eq(1000L), anyString(), anyString())).thenReturn(1);
    }

    @Test
    void deadLetterRecordsOnlyAnUnfinishedOrder() {
        service.recordDeadLetter(event);
        assertEquals("DEAD_LETTER", row.getStatus());
        verify(mapper).transition(1000L, "PENDING", "DEAD_LETTER");
    }

    @Test
    void lateDeadLetterCannotOverwriteCommittedOrCompensatingOrder() {
        for (String terminal : new String[]{"CREATED", "COMPENSATING", "COMPENSATED"}) {
            row.setStatus(terminal);
            service.recordDeadLetter(event);
            assertEquals(terminal, row.getStatus());
        }
        verify(mapper, never()).transition(anyLong(), anyString(), anyString());
    }

    @Test
    void existingHistoricalOrderIsMarkedCreated() {
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenReturn(new VoucherOrder().setStatus(1));
        service.recordDeadLetter(event);
        assertEquals("CREATED", row.getStatus());
    }

    @Test
    void mismatchedIdentityCannotOverwriteOrder() {
        row.setUserId(6L);
        assertThrows(IllegalStateException.class, () -> service.recordDeadLetter(event));
        verify(mapper, never()).transition(anyLong(), anyString(), anyString());
    }
}
