package com.hmdp.mq.consumer;

import com.hmdp.mq.config.RocketMqConfig;
import com.hmdp.mq.config.RocketMqConstants;
import com.hmdp.mq.config.RocketMqProperties;
import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.mq.support.MqMessageCodec;
import com.hmdp.mq.producer.SeckillTransactionListener;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.impl.VoucherOrderProcessService;
import com.hmdp.service.impl.SeckillCircuitBreaker;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/** 仅连接专用测试 Broker；验证应用的死信订阅和失败转死信链路。 */
@EnabledIfEnvironmentVariable(named = "HMDP_ROCKETMQ_DLQ_IT", matches = "true")
class VoucherOrderDeadLetterIT {
    @Test
    void shouldRegisterListenersAndMqConsumersAsDistinctBeans() {
        try (var context = new AnnotationConfigApplicationContext()) {
            RocketMqProperties properties = new RocketMqProperties();
            properties.setNameServer("127.0.0.1:19876");
            context.getEnvironment().getPropertySources().addFirst(
                    new MapPropertySource("test", Map.of("hmdp.cache.mq.enabled", "false")));
            context.registerBean(RocketMqProperties.class, () -> properties);
            context.registerBean("voucherOrderConsumer", VoucherOrderConsumer.class, () -> mock(VoucherOrderConsumer.class));
            context.registerBean("voucherOrderDeadLetterConsumer", VoucherOrderDeadLetterConsumer.class,
                    () -> mock(VoucherOrderDeadLetterConsumer.class));
            context.registerBean("orderTimeoutConsumer", OrderTimeoutConsumer.class, () -> mock(OrderTimeoutConsumer.class));
            context.registerBean(SeckillTransactionListener.class, () -> mock(SeckillTransactionListener.class));
            context.register(RocketMqConfig.class);
            context.refresh();
            assertInstanceOf(VoucherOrderDeadLetterConsumer.class, context.getBean("voucherOrderDeadLetterConsumer"));
            assertInstanceOf(DefaultMQPushConsumer.class, context.getBean("voucherOrderDeadLetterMqConsumer"));
            assertInstanceOf(VoucherOrderConsumer.class, context.getBean("voucherOrderConsumer"));
            assertInstanceOf(DefaultMQPushConsumer.class, context.getBean("voucherOrderMqConsumer"));
        }
    }

    @Test
    void shouldConsumeBrokerDeadLetterAndRecordItsOrder() throws Exception {
        RocketMqProperties properties = new RocketMqProperties();
        properties.setNameServer("127.0.0.1:19876");
        properties.setMaxReconsumeTimes(0);
        properties.setConsumeThreads(1);
        VoucherOrderMessage event = new VoucherOrderMessage(900001L, 5L, 10L);
        VoucherOrderProcessService processes = mock(VoucherOrderProcessService.class);
        CountDownLatch recorded = new CountDownLatch(1);
        doAnswer(i -> { recorded.countDown(); return null; }).when(processes).recordDeadLetter(event);
        IVoucherOrderService orders = mock(IVoucherOrderService.class);
        doThrow(new IllegalStateException("test failure")).when(orders).createVoucherOrder(any());
        RedissonClient redisson = mock(RedissonClient.class);
        RLock lock = mock(RLock.class);
        when(redisson.getLock("lock:order:10:5")).thenReturn(lock);
        when(lock.tryLock()).thenReturn(true);
        DefaultMQProducer producer = new DefaultMQProducer("hmdp-dlq-integration-producer");
        producer.setNamesrvAddr(properties.getNameServer());
        DefaultMQPushConsumer dead = null;
        DefaultMQPushConsumer normal = null;
        try {
            producer.start();
            producer.send(new Message(RocketMqConstants.ORDER_TOPIC, RocketMqConstants.ORDER_CREATE_TAG,
                    MqMessageCodec.encode(event)));
            RocketMqConfig config = new RocketMqConfig();
            // 死信主题初始不存在，验证消费者在主题随后由 Broker 创建时仍能发现它。
            dead = config.voucherOrderDeadLetterConsumer(properties,
                    new VoucherOrderDeadLetterConsumer(processes, mock(SeckillCircuitBreaker.class)));
            normal = config.voucherOrderConsumer(properties, new VoucherOrderConsumer(orders, redisson));
            assertTrue(recorded.await(55, TimeUnit.SECONDS), "死信未被自动登记");
            verify(processes).recordDeadLetter(event);
        } finally {
            if (normal != null) normal.shutdown();
            if (dead != null) dead.shutdown();
            producer.shutdown();
        }
    }
}
