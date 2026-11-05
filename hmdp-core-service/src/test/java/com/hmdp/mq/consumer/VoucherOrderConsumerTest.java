package com.hmdp.mq.consumer;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.mq.support.MqMessageCodec;
import com.hmdp.service.IVoucherOrderService;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.util.List;
import java.nio.charset.StandardCharsets;

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

        assertEquals(ConsumeConcurrentlyStatus.CONSUME_SUCCESS, consumer.consumeMessage(List.of(message()), null));

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
    void shouldAskForRedeliveryWhenLockIsBusy() {
        when(redisson.getLock("lock:order:10:5")).thenReturn(lock);

        assertEquals(ConsumeConcurrentlyStatus.RECONSUME_LATER, consumer.consumeMessage(List.of(message()), null));

        verifyNoInteractions(service);
        verify(lock, never()).unlock();
    }

    @Test
    void shouldReleaseLockAndAskForRedeliveryOnDatabaseFailure() {
        when(redisson.getLock("lock:order:10:5")).thenReturn(lock);
        when(lock.tryLock()).thenReturn(true);
        doThrow(new IllegalStateException("database unavailable")).when(service).createVoucherOrder(any());

        assertEquals(ConsumeConcurrentlyStatus.RECONSUME_LATER, consumer.consumeMessage(List.of(message()), null));

        verify(lock).unlock();
    }

    @Test
    void shouldRetryUnreadableMessageForDeadLetterHandling() {
        MessageExt broken = new MessageExt();
        broken.setBody(new byte[]{'x'});

        assertEquals(ConsumeConcurrentlyStatus.RECONSUME_LATER, consumer.consumeMessage(List.of(broken), null));

        verifyNoInteractions(service, redisson);
    }

    @Test
    void shouldReadLegacyMessagesThatContainRepublishCounter() {
        byte[] old = "{\"orderId\":1000,\"userId\":5,\"voucherId\":10,\"retryCount\":3}"
                .getBytes(StandardCharsets.UTF_8);
        assertEquals(message, MqMessageCodec.decode(old, VoucherOrderMessage.class));
    }

    private MessageExt message() {
        MessageExt raw = new MessageExt();
        raw.setTopic("topic");
        raw.setBody(MqMessageCodec.encode(message));
        raw.setMsgId("msg-1");
        return raw;
    }
}
