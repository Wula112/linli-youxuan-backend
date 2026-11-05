package com.hmdp.mq.consumer;

import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.mq.support.MqMessageCodec;
import com.hmdp.service.impl.VoucherOrderProcessService;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class VoucherOrderDeadLetterConsumerTest {
    private final VoucherOrderProcessService processes = mock(VoucherOrderProcessService.class);
    private final VoucherOrderDeadLetterConsumer consumer = new VoucherOrderDeadLetterConsumer(processes);
    private final VoucherOrderMessage event = new VoucherOrderMessage(1000L, 5L, 10L);

    @Test
    void acknowledgesOnlyAfterRecordingDeadLetter() {
        MessageExt message = message();
        assertEquals(ConsumeConcurrentlyStatus.CONSUME_SUCCESS, consumer.consumeMessage(List.of(message), null));
        verify(processes).recordDeadLetter(event);
    }

    @Test
    void retriesWhenDatabaseRecordingFails() {
        doThrow(new IllegalStateException("DB unavailable")).when(processes).recordDeadLetter(event);
        assertEquals(ConsumeConcurrentlyStatus.RECONSUME_LATER, consumer.consumeMessage(List.of(message()), null));
    }

    private MessageExt message() {
        MessageExt result = new MessageExt();
        result.setBody(MqMessageCodec.encode(event));
        return result;
    }
}
