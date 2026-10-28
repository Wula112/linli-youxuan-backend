package com.hmdp.mq.producer;

import com.hmdp.mq.config.OrderTimeoutMqConfig;
import com.hmdp.mq.config.VoucherOrderMqProperties;
import com.hmdp.mq.message.OrderTimeoutMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.core.retry.RetryException;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.retry.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.concurrent.TimeUnit;

/** 提交后发送；复用订单发布确认、Returns和有限重试配置，失败由数据库扫描兜底。 */
@Slf4j
@Component
public class OrderTimeoutPublisher {

    private final RabbitTemplate rabbitTemplate;
    private final VoucherOrderMqProperties properties;
    private final RetryTemplate retryTemplate;

    public OrderTimeoutPublisher(RabbitTemplate rabbitTemplate, VoucherOrderMqProperties properties) {
        this.rabbitTemplate = rabbitTemplate;
        this.properties = properties;
        retryTemplate = new RetryTemplate(RetryPolicy.builder()
                .maxRetries(properties.getProducerMaxAttempts() - 1L)
                .delay(properties.getRetryInterval()).excludes(InterruptedException.class).build());
    }

    public void publishAfterCommit(Long orderId) {
        OrderTimeoutMessage message = new OrderTimeoutMessage(orderId);
        if (!TransactionSynchronizationManager.isActualTransactionActive()
                || !TransactionSynchronizationManager.isSynchronizationActive()) {
            throw new IllegalStateException("必须在建单事务中注册延迟关单消息");
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                try {
                    publish(message);
                } catch (RetryException ex) {
                    if (ex.getCause() instanceof Error error) throw error;
                    if (ex.getCause() instanceof InterruptedException) Thread.currentThread().interrupt();
                    // 订单已经提交，不能因通知失败回滚订单或恢复预占。
                    log.error("延迟关单消息发送失败，等待数据库扫描兜底，orderId={}", orderId, ex);
                }
            }
        });
    }

    public void publish(OrderTimeoutMessage message) throws RetryException {
        retryTemplate.execute(() -> {
            CorrelationData correlation = new CorrelationData();
            rabbitTemplate.convertAndSend(OrderTimeoutMqConfig.DELAY_EXCHANGE, OrderTimeoutMqConfig.ROUTING_KEY,
                    message, correlation);
            CorrelationData.Confirm confirm = correlation.getFuture()
                    .get(properties.getConfirmTimeout().toMillis(), TimeUnit.MILLISECONDS);
            if (!confirm.ack() || correlation.getReturned() != null) {
                throw new IllegalStateException("延迟关单消息未确认或无法路由，orderId=" + message.orderId());
            }
            return null;
        });
    }
}
