package com.hmdp.mq.producer;

import com.hmdp.mq.config.RocketMqConstants;
import com.hmdp.mq.message.VoucherOrderMessage;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class VoucherOrderPublisherTest {

    private final TransactionMQProducer producer = mock(TransactionMQProducer.class);
    private final VoucherOrderPublisher publisher = new VoucherOrderPublisher(producer);
    private final VoucherOrderMessage message = new VoucherOrderMessage(1000L, 5L, 10L);

    @Test
    void shouldSendHalfMessageWithOrderPayload() throws Exception {
        completeLocalTransaction(0L);

        assertEquals(0L, publisher.sendInTransaction(message));

        ArgumentCaptor<Message> sent = ArgumentCaptor.forClass(Message.class);
        verify(producer).sendMessageInTransaction(sent.capture(), any(SeckillTxContext.class));
        assertEquals(RocketMqConstants.ORDER_TOPIC, sent.getValue().getTopic());
        assertEquals(RocketMqConstants.ORDER_CREATE_TAG, sent.getValue().getTags());
    }

    @Test
    void shouldReturnStockAndDuplicateResultsUnchanged() throws Exception {
        completeLocalTransaction(1L);
        assertEquals(1L, publisher.sendInTransaction(message));

        clearInvocations(producer);
        completeLocalTransaction(2L);
        assertEquals(2L, publisher.sendInTransaction(message));
    }

    @Test
    void shouldReturnUnknownWhenHalfMessageCannotBeSent() throws Exception {
        when(producer.sendMessageInTransaction(any(Message.class), any()))
                .thenThrow(new MQClientException("name server unreachable", null));

        assertEquals(-1L, publisher.sendInTransaction(message));
        verify(producer, never()).send(any(Message.class));
    }

    @Test
    void shouldRejectUnsuccessfulRepublish() throws Exception {
        when(producer.send(any(Message.class))).thenReturn(sendResult(SendStatus.FLUSH_DISK_TIMEOUT));

        assertThrows(IllegalStateException.class, () -> publisher.publish(message));
    }

    @Test
    void shouldRepublishAsNormalMessage() throws Exception {
        when(producer.send(any(Message.class))).thenReturn(sendResult(SendStatus.SEND_OK));

        publisher.publish(message);

        ArgumentCaptor<Message> sent = ArgumentCaptor.forClass(Message.class);
        verify(producer).send(sent.capture());
        assertEquals(RocketMqConstants.ORDER_CREATE_TAG, sent.getValue().getTags());
        // 补发不再触发本地事务
        verify(producer, never()).sendMessageInTransaction(any(Message.class), any());
    }

    private void completeLocalTransaction(long result) throws Exception {
        doAnswer(invocation -> {
            SeckillTxContext context = invocation.getArgument(1);
            context.complete(result);
            return null;
        }).when(producer).sendMessageInTransaction(any(Message.class), any());
    }

    private SendResult sendResult(SendStatus status) {
        SendResult result = new SendResult();
        result.setSendStatus(status);
        result.setMsgId("msg-1");
        return result;
    }
}
