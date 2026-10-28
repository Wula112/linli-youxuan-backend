package com.hmdp.mq;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.mq.consumer.VoucherOrderConsumer;
import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.service.IVoucherOrderService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class VoucherOrderConsumerTest {

    private final IVoucherOrderService service = mock(IVoucherOrderService.class);
    private final RedissonClient redisson = mock(RedissonClient.class);
    private final RLock lock = mock(RLock.class);
    private final VoucherOrderConsumer consumer = new VoucherOrderConsumer(service, redisson);
    private final VoucherOrderMessage message = new VoucherOrderMessage(1000L, 5L, 10L);

    @Test
    void shouldHoldLockUntilTransactionalServiceReturns() {
        when(redisson.getLock("lock:order:10:5")).thenReturn(lock);
        when(lock.tryLock()).thenReturn(true);
        consumer.consume(message);
        var sequence = inOrder(lock, service);
        sequence.verify(lock).tryLock();
        ArgumentCaptor<VoucherOrder> order = ArgumentCaptor.forClass(VoucherOrder.class);
        sequence.verify(service).createVoucherOrder(order.capture());
        sequence.verify(lock).unlock();
        assertEquals(1000L, order.getValue().getId());
        assertEquals(5L, order.getValue().getUserId());
        assertEquals(10L, order.getValue().getVoucherId());
    }

    @Test
    void shouldThrowWhenLockIsBusySoContainerRetries() {
        when(redisson.getLock("lock:order:10:5")).thenReturn(lock);
        assertThrows(IllegalStateException.class, () -> consumer.consume(message));
        verifyNoInteractions(service);
        verify(lock, never()).unlock();
    }

    @Test
    void shouldReleaseLockAndPropagateDatabaseFailure() {
        when(redisson.getLock("lock:order:10:5")).thenReturn(lock);
        when(lock.tryLock()).thenReturn(true);
        doThrow(new IllegalStateException("database unavailable")).when(service).createVoucherOrder(any());
        assertThrows(IllegalStateException.class, () -> consumer.consume(message));
        verify(lock).unlock();
    }
}
