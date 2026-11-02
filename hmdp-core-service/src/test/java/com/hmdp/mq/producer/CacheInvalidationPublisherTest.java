package com.hmdp.mq.producer;

import com.hmdp.mq.message.CacheInvalidationMessage;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import static com.hmdp.common.constant.RedisConstants.CACHE_SHOP_TYPE_KEY;
import static com.hmdp.common.constant.RedisConstants.CACHE_SHOP_VOUCHER_KEY;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class CacheInvalidationPublisherTest {

    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final TransactionMQProducer producer = mock(TransactionMQProducer.class);
    private final CacheInvalidationPublisher publisher = new CacheInvalidationPublisher(redis, producer);

    @BeforeEach
    void stubBroker() throws Exception {
        SendResult result = new SendResult();
        result.setSendStatus(SendStatus.SEND_OK);
        when(producer.send(any(Message.class))).thenReturn(result);
    }

    @Test
    void shouldWaitForCommitThenDeleteRedisBeforePublishing() throws Exception {
        try (AnnotationConfigApplicationContext context = context()) {
            TransactionTemplate transaction = new TransactionTemplate(new TestTransactionManager());
            transaction.executeWithoutResult(status -> {
                context.publishEvent(CacheInvalidationMessage.voucherList(1L));
                verifyNoInteractions(redis, producer);
            });
        }
        var order = inOrder(redis, producer);
        order.verify(redis).delete(CACHE_SHOP_VOUCHER_KEY + 1);
        order.verify(producer).send(any(Message.class));
    }

    @Test
    void shouldDiscardEventOnRollback() throws Exception {
        try (AnnotationConfigApplicationContext context = context()) {
            TransactionTemplate transaction = new TransactionTemplate(new TestTransactionManager());
            transaction.executeWithoutResult(status -> {
                context.publishEvent(CacheInvalidationMessage.shopType());
                status.setRollbackOnly();
            });
        }
        verifyNoInteractions(redis, producer);
    }

    @Test
    void shouldExecuteImmediatelyWithoutTransaction() throws Exception {
        try (AnnotationConfigApplicationContext context = context()) {
            context.publishEvent(CacheInvalidationMessage.shopType());
            verify(redis).delete(CACHE_SHOP_TYPE_KEY);
            verify(producer).send(any(Message.class));
        }
    }

    @Test
    void shouldStillBroadcastWhenRedisDeletionFails() throws Exception {
        when(redis.delete(CACHE_SHOP_TYPE_KEY)).thenThrow(new IllegalStateException("Redis unavailable"));

        publisher.onCacheChanged(CacheInvalidationMessage.shopType());

        verify(redis).delete(CACHE_SHOP_TYPE_KEY);
        verify(producer).send(any(Message.class));
    }

    @Test
    void shouldNotFailCommittedBusinessWhenMessageCannotBeSent() throws Exception {
        when(producer.send(any(Message.class))).thenThrow(new MQClientException("broker unavailable", null));

        assertDoesNotThrow(() -> publisher.onCacheChanged(CacheInvalidationMessage.shopType()));
        verify(producer).send(any(Message.class));
        verify(redis).delete(anyString());
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
