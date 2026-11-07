package com.hmdp.mq.consumer;

import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.mq.support.MqMessageCodec;
import com.hmdp.service.impl.VoucherOrderProcessService;
import com.hmdp.service.impl.SeckillCircuitBreaker;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class VoucherOrderDeadLetterConsumerTest {
    private final VoucherOrderProcessService processes = mock(VoucherOrderProcessService.class);
    private final SeckillCircuitBreaker circuit = mock(SeckillCircuitBreaker.class);
    private final VoucherOrderDeadLetterConsumer consumer = new VoucherOrderDeadLetterConsumer(processes, circuit);
    private final VoucherOrderMessage event = new VoucherOrderMessage(1000L, 5L, 10L);

    @Test
    void acknowledgesOnlyAfterRecordingDeadLetter() {
        MessageExt message = message();
        assertEquals(ConsumeConcurrentlyStatus.CONSUME_SUCCESS, consumer.consumeMessage(List.of(message), null));
        var sequence = inOrder(circuit, processes);
        sequence.verify(circuit).trip(any());
        sequence.verify(processes).recordDeadLetter(event);
    }

    @Test
    void retriesWhenDatabaseRecordingFails() {
        doThrow(new IllegalStateException("DB unavailable")).when(processes).recordDeadLetter(event);
        assertEquals(ConsumeConcurrentlyStatus.RECONSUME_LATER, consumer.consumeMessage(List.of(message()), null));
    }

    @Test
    void tripsEvenWhenMessageCannotBeDecoded() {
        MessageExt message = new MessageExt();
        message.setBody(new byte[]{1, 2, 3});
        assertEquals(ConsumeConcurrentlyStatus.RECONSUME_LATER, consumer.consumeMessage(List.of(message), null));
        verify(circuit).trip(any());
        verifyNoInteractions(processes);
    }

    @Test
    void doesNotAcknowledgeWhenCircuitCannotBePersisted() {
        doThrow(new IllegalStateException("Redis unavailable")).when(circuit).trip(any());
        assertEquals(ConsumeConcurrentlyStatus.RECONSUME_LATER, consumer.consumeMessage(List.of(message()), null));
        verifyNoInteractions(processes);
    }

    private MessageExt message() {
        MessageExt result = new MessageExt();
        result.setBody(MqMessageCodec.encode(event));
        return result;
    }
}
