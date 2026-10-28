package com.hmdp.mq.producer;

import com.hmdp.mq.config.CacheMqProperties;
import com.hmdp.mq.message.CacheInvalidationMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.retry.RetryException;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.retry.RetryTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 在业务事务提交后删除 Redis 缓存，并通过 fanout exchange 广播失效消息。
 *
 * <p>发布流程保持“先删 Redis、再发消息”的顺序。删除操作是幂等的，重试时重复执行也不会产生副作用。
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "hmdp.cache.mq", name = "enabled", havingValue = "true", matchIfMissing = true)
public class CacheInvalidationPublisher {

    /**
     * fanout exchange 会忽略路由键，这里显式保留空字符串，避免调用处出现魔法值。
     */
    private static final String FANOUT_ROUTING_KEY = "";

    private final StringRedisTemplate stringRedisTemplate;
    private final RabbitTemplate rabbitTemplate;
    private final CacheMqProperties properties;
    private final RetryTemplate retryTemplate;

    public CacheInvalidationPublisher(StringRedisTemplate stringRedisTemplate,
            RabbitTemplate rabbitTemplate,
            CacheMqProperties properties) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.rabbitTemplate = rabbitTemplate;
        this.properties = properties;
        // producerMaxAttempts 包含首次执行，而 RetryPolicy 的 maxRetries 只统计重试次数。
        this.retryTemplate = new RetryTemplate(RetryPolicy.builder()
                .maxRetries(properties.getProducerMaxAttempts() - 1L)
                .delay(properties.getRetryInterval())
                .excludes(InterruptedException.class)
                .build());
    }

    // 事务回滚不会执行；无事务时，数据库操作已由调用方完成，直接执行失效。
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT, fallbackExecution = true)
    public void onCacheChanged(CacheInvalidationMessage event) {
        // 第一阶段：删除 Redis
        try {
            stringRedisTemplate.delete(event.redisKey());
        } catch (Exception ex) {
            // Redis 删除失败也继续发送消息，尽可能清除各实例本地缓存
            log.error("Redis 缓存删除失败，key={}", event.redisKey(), ex);
            // 生产环境还应记录待补偿任务
        }

        // 第二阶段：只重试 RabbitMQ 发布
        try {
            retryTemplate.execute(() -> {
                publishAndConfirm(event);
                return null;
            });
        } catch (RetryException ex) {
            // RetryException 会把 Error 也包装成普通异常，这里恢复原有语义直接抛出。
            if (ex.getCause() instanceof Error error) {
                throw error;
            }
            // RetryTemplate 在 back-off 被中断时会恢复线程中断标记；
            // 首次执行中拿到 InterruptedException 时则保留在 cause 中。
            if (Thread.currentThread().isInterrupted() || ex.getCause() instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                log.error("缓存失效消息发送被中断，event={}", event, ex);
                return;
            }
            // 数据库已经提交，此处只记录失败，不能再回滚业务事务。
            log.error("缓存失效消息发送重试耗尽，需要补发，event={}", event, ex);
        }
    }

    /**
     * 发布消息并等待 Broker 的发布确认。
     */
    private void publishAndConfirm(CacheInvalidationMessage event)
            throws InterruptedException, ExecutionException, TimeoutException {

        CorrelationData correlation = new CorrelationData();
        rabbitTemplate.convertAndSend(properties.getExchange(), FANOUT_ROUTING_KEY, event, correlation);

        CorrelationData.Confirm confirm = correlation.getFuture()
                .get(properties.getConfirmTimeout().toMillis(), TimeUnit.MILLISECONDS);
        if (!confirm.ack()) {
            throw new IllegalStateException(
                    "Broker NACK: " + confirm.reason()
            );
        }

        if (correlation.getReturned() != null) {
            throw new IllegalStateException("消息无法路由");
        }
    }
}
