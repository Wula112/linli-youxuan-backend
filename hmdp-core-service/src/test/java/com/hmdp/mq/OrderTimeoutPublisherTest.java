package com.hmdp.mq;

import com.hmdp.config.VoucherOrderProperties;
import com.hmdp.mq.config.OrderTimeoutMqConfig;
import com.hmdp.framework.mq.config.RabbitConfig;
import com.hmdp.mq.config.VoucherOrderMqProperties;
import com.hmdp.mq.message.OrderTimeoutMessage;
import com.hmdp.mq.producer.OrderTimeoutPublisher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.core.retry.RetryException;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class OrderTimeoutPublisherTest {

    private final RabbitTemplate rabbit = mock(RabbitTemplate.class);
    private final VoucherOrderMqProperties properties = new VoucherOrderMqProperties();
    private final OrderTimeoutMessage message = new OrderTimeoutMessage(1000L);
    private OrderTimeoutPublisher publisher;

    @BeforeEach
    void setUp() {
        properties.setRetryInterval(Duration.ofMillis(1));
        properties.setConfirmTimeout(Duration.ofMillis(1));
        publisher = new OrderTimeoutPublisher(rabbit, properties);
        Thread.interrupted();
    }

    @AfterEach
    void clearTransaction() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
        TransactionSynchronizationManager.setActualTransactionActive(false);
        Thread.interrupted();
    }

    @Test
    void shouldPublishOnlyAfterCommitAndNotOnRollback() {
        confirm(true, false);
        transaction();
        publisher.publishAfterCommit(1000L);
        verifyNoInteractions(rabbit);
        TransactionSynchronizationManager.getSynchronizations()
                .forEach(sync -> sync.afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK));
        verifyNoInteractions(rabbit);
        TransactionSynchronizationManager.getSynchronizations().forEach(TransactionSynchronization::afterCommit);
        verifyAttempts(1);
    }

    @Test
    void shouldRequireActualOrderTransaction() {
        assertThrows(IllegalStateException.class, () -> publisher.publishAfterCommit(1000L));
        verifyNoInteractions(rabbit);
    }

    @Test
    void shouldRetryNegativeConfirmAndReturnedMessage() {
        confirm(false, false);
        assertThrows(RetryException.class, () -> publisher.publish(message));
        verifyAttempts(3);
        clearInvocations(rabbit);
        confirm(true, true);
        assertThrows(RetryException.class, () -> publisher.publish(message));
        verifyAttempts(3);
    }

    @Test
    void shouldBoundConfirmTimeoutAndKeepCommittedOrderSuccessful() {
        transaction();
        publisher.publishAfterCommit(1000L);
        assertDoesNotThrow(() -> TransactionSynchronizationManager.getSynchronizations()
                .forEach(TransactionSynchronization::afterCommit));
        verifyAttempts(3);
    }

    @Test
    void shouldDeclareFixedMessageTtlAndSeparateFailureDeadLetterRoute() {
        VoucherOrderProperties orderProperties = new VoucherOrderProperties();
        var topology = new OrderTimeoutMqConfig().orderTimeoutDeclarables(orderProperties);
        Queue delay = topology.getDeclarablesByType(Queue.class).stream()
                .filter(queue -> queue.getName().equals(OrderTimeoutMqConfig.DELAY_QUEUE)).findFirst().orElseThrow();
        Queue close = topology.getDeclarablesByType(Queue.class).stream()
                .filter(queue -> queue.getName().equals(OrderTimeoutMqConfig.CLOSE_QUEUE)).findFirst().orElseThrow();
        assertTrue(delay.isDurable());
        assertEquals(900000L, delay.getArguments().get("x-message-ttl"));
        assertEquals(OrderTimeoutMqConfig.CLOSE_EXCHANGE, delay.getArguments().get("x-dead-letter-exchange"));
        assertEquals(OrderTimeoutMqConfig.DEAD_EXCHANGE, close.getArguments().get("x-dead-letter-exchange"));
        var converter = new RabbitConfig().messageConverter();
        Message converted = converter.toMessage(message, new MessageProperties());
        assertEquals(MessageDeliveryMode.PERSISTENT, converted.getMessageProperties().getDeliveryMode());
        assertEquals(message, converter.fromMessage(converted));
    }

    private void transaction() {
        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(true);
    }

    private void confirm(boolean ack, boolean returned) {
        doAnswer(invocation -> {
            CorrelationData correlation = invocation.getArgument(3);
            if (returned) correlation.setReturned(new ReturnedMessage(new Message(new byte[0]), 312,
                    "NO_ROUTE", "exchange", "key"));
            correlation.getFuture().complete(new CorrelationData.Confirm(ack, "test"));
            return null;
        }).when(rabbit).convertAndSend(anyString(), anyString(), any(OrderTimeoutMessage.class), any(CorrelationData.class));
    }

    private void verifyAttempts(int times) {
        verify(rabbit, times(times)).convertAndSend(eq(OrderTimeoutMqConfig.DELAY_EXCHANGE),
                eq(OrderTimeoutMqConfig.ROUTING_KEY), eq(message), any(CorrelationData.class));
    }
}
