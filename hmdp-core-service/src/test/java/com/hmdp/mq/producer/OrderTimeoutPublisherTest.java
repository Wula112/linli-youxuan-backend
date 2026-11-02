package com.hmdp.mq.producer;

import com.hmdp.config.VoucherOrderProperties;
import com.hmdp.mq.config.RocketMqConstants;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OrderTimeoutPublisherTest {

    private final TransactionMQProducer producer = mock(TransactionMQProducer.class);
    private final VoucherOrderProperties orderProperties = new VoucherOrderProperties();
    private final OrderTimeoutPublisher publisher = new OrderTimeoutPublisher(producer, orderProperties);

    @AfterEach
    void clearTransaction() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
        TransactionSynchronizationManager.setActualTransactionActive(false);
    }

    @Test
    void shouldPublishDelayedMessageWithPayTimeout() throws Exception {
        when(producer.send(any(Message.class))).thenReturn(sendOk());

        publisher.publish(1000L);

        ArgumentCaptor<Message> sent = ArgumentCaptor.forClass(Message.class);
        verify(producer).send(sent.capture());
        assertEquals(RocketMqConstants.ORDER_TIMEOUT_TAG, sent.getValue().getTags());
        assertEquals(orderProperties.getPayTimeout().toSeconds(), sent.getValue().getDelayTimeSec());
    }

    @Test
    void shouldRequireActualOrderTransaction() {
        assertThrows(IllegalStateException.class, () -> publisher.publishAfterCommit(1000L));
        verifyNoInteractions(producer);
    }

    @Test
    void shouldSendOnlyAfterCommit() throws Exception {
        when(producer.send(any(Message.class))).thenReturn(sendOk());
        beginTransaction();

        publisher.publishAfterCommit(1000L);
        verifyNoInteractions(producer);

        TransactionSynchronizationManager.getSynchronizations()
                .forEach(sync -> sync.afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK));
        verifyNoInteractions(producer);

        TransactionSynchronizationManager.getSynchronizations().forEach(TransactionSynchronization::afterCommit);
        verify(producer).send(any(Message.class));
    }

    @Test
    void shouldKeepCommittedOrderWhenDelayMessageCannotBeSent() throws Exception {
        when(producer.send(any(Message.class))).thenThrow(new MQClientException("broker unavailable", null));
        beginTransaction();

        publisher.publishAfterCommit(1000L);

        assertDoesNotThrow(() -> TransactionSynchronizationManager.getSynchronizations()
                .forEach(TransactionSynchronization::afterCommit));
        verify(producer).send(any(Message.class));
    }

    private void beginTransaction() {
        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(true);
    }

    private SendResult sendOk() {
        SendResult result = new SendResult();
        result.setSendStatus(SendStatus.SEND_OK);
        return result;
    }
}
