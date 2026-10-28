package com.hmdp.mq.producer;

import com.hmdp.mq.config.VoucherOrderMqConfig;
import com.hmdp.mq.config.VoucherOrderMqProperties;
import com.hmdp.mq.message.VoucherOrderMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.retry.RetryException;
import org.springframework.core.retry.RetryTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/** 有限重试并等待发布确认；失败继续抛出，由下单入口协调预扣回退。 */
@Component
public class VoucherOrderPublisher {

    private final RabbitTemplate rabbitTemplate;
    private final VoucherOrderMqProperties properties;
    private final RetryTemplate retryTemplate;

    public VoucherOrderPublisher(RabbitTemplate rabbitTemplate, VoucherOrderMqProperties properties) {
        this.rabbitTemplate = rabbitTemplate;
        this.properties = properties;
        retryTemplate = new RetryTemplate(RetryPolicy.builder()
                .maxRetries(properties.getProducerMaxAttempts() - 1L)
                .delay(properties.getRetryInterval()).excludes(InterruptedException.class).build());
    }

    public void publish(VoucherOrderMessage message) throws RetryException {
        retryTemplate.execute(() -> {
            CorrelationData correlation = new CorrelationData();
            rabbitTemplate.convertAndSend(VoucherOrderMqConfig.EXCHANGE, VoucherOrderMqConfig.ROUTING_KEY,
                    message, correlation);
            CorrelationData.Confirm confirm = correlation.getFuture()
                    .get(properties.getConfirmTimeout().toMillis(), TimeUnit.MILLISECONDS);
            if (!confirm.ack() || correlation.getReturned() != null) {
                throw new IllegalStateException("秒杀订单消息未确认或无法路由，orderId=" + message.orderId());
            }
            return null;
        });
    }
}
