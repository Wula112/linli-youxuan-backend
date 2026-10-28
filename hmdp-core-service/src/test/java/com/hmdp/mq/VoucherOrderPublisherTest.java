package com.hmdp.mq;

import com.hmdp.mq.config.VoucherOrderMqConfig;
import com.hmdp.mq.config.VoucherOrderMqProperties;
import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.mq.producer.VoucherOrderPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.core.retry.RetryException;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class VoucherOrderPublisherTest {

    private final RabbitTemplate rabbit = mock(RabbitTemplate.class);
    private final VoucherOrderMqProperties properties = new VoucherOrderMqProperties();
    private final VoucherOrderMessage message = new VoucherOrderMessage(1000L, 5L, 10L);
    private VoucherOrderPublisher publisher;

    @BeforeEach
    void setUp() {
        properties.setRetryInterval(Duration.ofMillis(1));
        properties.setConfirmTimeout(Duration.ofMillis(1));
        publisher = new VoucherOrderPublisher(rabbit, properties);
    }

    private void confirm(boolean ack, boolean returned) {
        doAnswer(invocation -> {
            CorrelationData correlation = invocation.getArgument(3);
            if (returned) {
                correlation.setReturned(new ReturnedMessage(new Message(new byte[0]), 312, "NO_ROUTE", "exchange", "key"));
            }
            correlation.getFuture().complete(new CorrelationData.Confirm(ack, "test"));
            return null;
        }).when(rabbit).convertAndSend(anyString(), anyString(), any(VoucherOrderMessage.class), any(CorrelationData.class));
    }

    private void verifyAttempts(int attempts) {
        verify(rabbit, times(attempts)).convertAndSend(eq(VoucherOrderMqConfig.EXCHANGE),
                eq(VoucherOrderMqConfig.ROUTING_KEY), eq(message), any(CorrelationData.class));
    }

    @Test
    void shouldWaitForPositiveConfirm() throws Exception {
        confirm(true, false);
        publisher.publish(message);
        verifyAttempts(1);
    }

    @Test
    void shouldRetrySameOrderAndPropagateNegativeConfirm() {
        confirm(false, false);
        assertThrows(RetryException.class, () -> publisher.publish(message));
        verifyAttempts(3);
    }

    @Test
    void shouldNotAcceptReturnedMessageDespiteAck() {
        confirm(true, true);
        assertThrows(RetryException.class, () -> publisher.publish(message));
        verifyAttempts(3);
    }

    @Test
    void shouldBoundRetriesOnConfirmTimeout() {
        assertThrows(RetryException.class, () -> publisher.publish(message));
        verifyAttempts(3);
    }
}
