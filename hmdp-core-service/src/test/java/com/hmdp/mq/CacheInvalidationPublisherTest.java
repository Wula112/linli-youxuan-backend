package com.hmdp.mq;

import com.hmdp.mq.config.CacheMqProperties;
import com.hmdp.mq.message.CacheInvalidationMessage;
import com.hmdp.mq.producer.CacheInvalidationPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;

import static com.hmdp.common.constant.RedisConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CacheInvalidationPublisherTest {

    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final RabbitTemplate rabbit = mock(RabbitTemplate.class);
    private final CacheMqProperties properties = new CacheMqProperties();
    private CacheInvalidationPublisher publisher;

    @BeforeEach
    void setUp() {
        properties.setRetryInterval(Duration.ofMillis(1));
        properties.setConfirmTimeout(Duration.ofMillis(10));
        // RetryTemplate 在构造时读取重试间隔，必须在调整配置后创建发布者。
        publisher = new CacheInvalidationPublisher(redis, rabbit, properties);
    }

    private void confirmSuccess() {
        doAnswer(invocation -> {
            CorrelationData correlation = invocation.getArgument(3);
            correlation.getFuture().complete(new CorrelationData.Confirm(true, null));
            return null;
        }).when(rabbit).convertAndSend(anyString(), anyString(), any(CacheInvalidationMessage.class),
                any(CorrelationData.class));
    }

    private void verifyPublished(int expected) {
        verify(rabbit, times(expected)).convertAndSend(eq(properties.getExchange()), eq(""),
                any(CacheInvalidationMessage.class), any(CorrelationData.class));
    }

    @Test
    void shouldWaitForCommitThenDeleteRedisBeforePublishing() {
        confirmSuccess();
        try (AnnotationConfigApplicationContext context = context()) {
            TransactionTemplate transaction = new TransactionTemplate(new TestTransactionManager());
            transaction.executeWithoutResult(status -> {
                context.publishEvent(CacheInvalidationMessage.voucherList(1L));
                verifyNoInteractions(redis, rabbit);
            });
        }
        var order = inOrder(redis, rabbit);
        order.verify(redis).delete(CACHE_SHOP_VOUCHER_KEY + 1);
        order.verify(rabbit).convertAndSend(eq(properties.getExchange()), eq(""),
                any(CacheInvalidationMessage.class), any(CorrelationData.class));
    }

    @Test
    void shouldDiscardEventOnRollback() {
        try (AnnotationConfigApplicationContext context = context()) {
            TransactionTemplate transaction = new TransactionTemplate(new TestTransactionManager());
            transaction.executeWithoutResult(status -> {
                context.publishEvent(CacheInvalidationMessage.shopType());
                status.setRollbackOnly();
            });
        }
        verifyNoInteractions(redis, rabbit);
    }

    @Test
    void shouldExecuteImmediatelyWithoutTransaction() {
        confirmSuccess();
        try (AnnotationConfigApplicationContext context = context()) {
            context.publishEvent(CacheInvalidationMessage.shopType());
            verify(redis).delete(CACHE_SHOP_TYPE_KEY);
            verifyPublished(1);
        }
    }

    @Test
    void shouldStillBroadcastWhenRedisDeletionFails() {
        confirmSuccess();
        when(redis.delete(CACHE_SHOP_TYPE_KEY)).thenThrow(new IllegalStateException("Redis unavailable"))
                .thenReturn(true);
        publisher.onCacheChanged(CacheInvalidationMessage.shopType());
        verify(redis).delete(CACHE_SHOP_TYPE_KEY);
        verifyPublished(1);
    }

    @Test
    void shouldNotRetryRedisDeletionWhileRetryingPublication() {
        when(redis.delete(anyString())).thenThrow(new IllegalStateException("Redis unavailable"));
        assertDoesNotThrow(() -> publisher.onCacheChanged(CacheInvalidationMessage.shopType()));
        verify(redis).delete(CACHE_SHOP_TYPE_KEY);
        verifyPublished(3);
    }

    @Test
    void shouldRetryNegativeConfirm() {
        doAnswer(invocation -> {
            CorrelationData correlation = invocation.getArgument(3);
            correlation.getFuture().complete(new CorrelationData.Confirm(false, "nack"));
            return null;
        }).when(rabbit).convertAndSend(anyString(), anyString(), any(CacheInvalidationMessage.class),
                any(CorrelationData.class));
        assertDoesNotThrow(() -> publisher.onCacheChanged(CacheInvalidationMessage.shopType()));
        verifyPublished(3);
    }

    @Test
    void shouldTreatReturnedMessageAsFailureEvenWithPositiveConfirm() {
        doAnswer(invocation -> {
            CorrelationData correlation = invocation.getArgument(3);
            correlation.setReturned(new ReturnedMessage(new Message(new byte[0]), 312, "NO_ROUTE", "exchange", ""));
            correlation.getFuture().complete(new CorrelationData.Confirm(true, null));
            return null;
        }).when(rabbit).convertAndSend(anyString(), anyString(), any(CacheInvalidationMessage.class),
                any(CorrelationData.class));
        publisher.onCacheChanged(CacheInvalidationMessage.shopType());
        verifyPublished(3);
    }

    @Test
    void shouldBoundRetriesWhenConfirmTimesOut() {
        properties.setConfirmTimeout(Duration.ofMillis(1));
        publisher.onCacheChanged(CacheInvalidationMessage.shopType());
        verifyPublished(3);
    }

    private AnnotationConfigApplicationContext context() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.register(TransactionConfiguration.class);
        context.registerBean(CacheInvalidationPublisher.class, () -> publisher);
        context.refresh();
        return context;
    }

    @Configuration
    @EnableTransactionManagement
    static class TransactionConfiguration {
    }

    // 只驱动真实 Spring 事务生命周期，不连接外部数据库。
    private static class TestTransactionManager extends AbstractPlatformTransactionManager {
        @Override protected Object doGetTransaction() { return new Object(); }
        @Override protected void doBegin(Object transaction, TransactionDefinition definition) { }
        @Override protected void doCommit(DefaultTransactionStatus status) { }
        @Override protected void doRollback(DefaultTransactionStatus status) { }
    }
}
