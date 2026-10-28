package com.hmdp.mq.consumer;

import com.hmdp.mq.config.CacheMqProperties;
import com.hmdp.mq.message.CacheInvalidationMessage;
import com.hmdp.cache.ShopTypeCacheService;
import com.hmdp.cache.VoucherListCacheService;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.retry.RetryException;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.retry.RetryTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * 消费失效广播，只清理当前实例的本地缓存。
 *
 * <p>消息体由共用 JSON 转换器反序列化为 {@link CacheInvalidationMessage}，
 * 非法 JSON 由监听容器直接拒绝并进入死信队列。
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "hmdp.cache.mq", name = "enabled", havingValue = "true", matchIfMissing = true)
public class CacheInvalidationConsumer {

    private final ShopTypeCacheService shopTypeCacheService;
    private final VoucherListCacheService voucherListCacheService;
    private final RetryTemplate retryTemplate;

    public CacheInvalidationConsumer(ShopTypeCacheService shopTypeCacheService,
            VoucherListCacheService voucherListCacheService,
            CacheMqProperties properties) {
        this.shopTypeCacheService = shopTypeCacheService;
        this.voucherListCacheService = voucherListCacheService;
        // consumerMaxAttempts 包含首次执行，而 RetryPolicy 的 maxRetries 只统计重试次数。
        this.retryTemplate = new RetryTemplate(RetryPolicy.builder()
                .maxRetries(properties.getConsumerMaxAttempts() - 1L)
                .delay(properties.getRetryInterval())
                .excludes(InterruptedException.class)
                .build());
    }

    @RabbitListener(queues = "#{@cacheInvalidationQueue.name}",
            containerFactory = "manualAckFactory")
    public void onMessage(@Payload CacheInvalidationMessage event, Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        try {
            retryTemplate.execute(() -> {
                invalidateLocalCache(event);
                return null;
            });
            channel.basicAck(deliveryTag, false);
        } catch (RetryException ex) {
            // RetryException 会把 Error 也包装成普通异常，这里恢复原有语义直接抛出。
            if (ex.getCause() instanceof Error error) {
                throw error;
            }
            // RetryTemplate 在 back-off 被中断时会恢复线程中断标记；
            // 首次执行中拿到 InterruptedException 时则保留在 cause 中。
            if (Thread.currentThread().isInterrupted() || ex.getCause() instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                log.error("清理本地缓存被中断，requeue消息，event={}", event, ex);
                channel.basicNack(deliveryTag, false, true);
                return;
            }
            // 重试耗尽后不重新入队，由实例队列绑定的 DLX 路由到死信队列。
            log.error("清理本地缓存重试耗尽，消息进入死信队列，event={}", event, ex);
            channel.basicReject(deliveryTag, false);
        }
    }

    private void invalidateLocalCache(CacheInvalidationMessage event) {
        // 校验缓存类型和业务标识，非法消息同样走重试与死信。
        event.redisKey();
        if (CacheInvalidationMessage.SHOP_TYPE.equals(event.getCacheName())) {
            shopTypeCacheService.invalidateLocalCache();
        } else {
            voucherListCacheService.invalidateLocalCache(event.getShopId());
        }
    }
}
