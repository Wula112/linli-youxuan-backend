package com.hmdp.mq.config;

import com.hmdp.mq.consumer.CacheInvalidationConsumer;
import com.hmdp.mq.consumer.OrderTimeoutConsumer;
import com.hmdp.mq.consumer.VoucherOrderConsumer;
import com.hmdp.mq.consumer.VoucherOrderDeadLetterConsumer;
import com.hmdp.mq.producer.SeckillTransactionListener;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

/**
 * RocketMQ 客户端装配：一条事务生产者承担全部发送；消费者分别处理下单、死信、关单与缓存失效。
 *
 * <p>消费者失败重投次数由 max-reconsume-times 控制，耗尽后进入 %DLQ%&lt;consumer-group&gt;。
 */
@Configuration
public class RocketMqConfig {

    /** 实例标识只用于缓存失效的消费者组，保证组内仅此一个实例，等价于原来的实例独立队列。 */
    private static final String CACHE_INSTANCE_ID = UUID.randomUUID().toString();

    @Bean(destroyMethod = "shutdown")
    public TransactionMQProducer rocketMqProducer(RocketMqProperties properties,
            SeckillTransactionListener transactionListener) throws MQClientException {
        TransactionMQProducer producer = new TransactionMQProducer(properties.getProducerGroup());
        producer.setNamesrvAddr(properties.getNameServer());
        producer.setSendMsgTimeout((int) properties.getSendTimeout().toMillis());
        producer.setRetryTimesWhenSendFailed(properties.getRetryTimesWhenSendFailed());
        producer.setCheckThreadPoolMinSize(1);
        producer.setCheckThreadPoolMaxSize(properties.getTransactionCheckThreads());
        producer.setTransactionListener(transactionListener);
        producer.start();
        return producer;
    }

    /** 下单消息：共享消费者组，一条消息只由一个实例处理。 */
    @Bean(name = "voucherOrderMqConsumer", destroyMethod = "shutdown")
    public DefaultMQPushConsumer voucherOrderConsumer(RocketMqProperties properties,
            VoucherOrderConsumer listener) throws MQClientException {
        return start(properties, RocketMqConstants.ORDER_CONSUMER_GROUP, RocketMqConstants.ORDER_TOPIC,
                RocketMqConstants.ORDER_CREATE_TAG, ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET, listener);
    }

    /** 独立组读取下单消费者组死信，首次启动从最早仍保留的消息开始。 */
    @Bean(name = "voucherOrderDeadLetterMqConsumer", destroyMethod = "shutdown")
    public DefaultMQPushConsumer voucherOrderDeadLetterConsumer(RocketMqProperties properties,
            VoucherOrderDeadLetterConsumer listener) throws MQClientException {
        return start(properties, RocketMqConstants.ORDER_DEAD_LETTER_CONSUMER_GROUP,
                RocketMqConstants.ORDER_DEAD_LETTER_TOPIC, "*", ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET, listener);
    }

    /** 延迟关单消息：到点才会被投递，处理逻辑与数据库扫描兜底完全一致。 */
    @Bean(name = "orderTimeoutMqConsumer", destroyMethod = "shutdown")
    public DefaultMQPushConsumer orderTimeoutConsumer(RocketMqProperties properties,
            OrderTimeoutConsumer listener) throws MQClientException {
        return start(properties, RocketMqConstants.TIMEOUT_CONSUMER_GROUP, RocketMqConstants.ORDER_TOPIC,
                RocketMqConstants.ORDER_TIMEOUT_TAG, ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET, listener);
    }

    /** 缓存失效广播：每实例一个消费者组，新实例只关心启动之后的消息。 */
    @Bean(name = "cacheInvalidationMqConsumer", destroyMethod = "shutdown")
    @ConditionalOnProperty(prefix = "hmdp.cache.mq", name = "enabled", havingValue = "true", matchIfMissing = true)
    public DefaultMQPushConsumer cacheInvalidationConsumer(RocketMqProperties properties,
            CacheInvalidationConsumer listener) throws MQClientException {
        return start(properties, RocketMqConstants.CACHE_CONSUMER_GROUP_PREFIX + CACHE_INSTANCE_ID,
                RocketMqConstants.CACHE_TOPIC, RocketMqConstants.CACHE_TAG,
                ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET, listener);
    }

    private DefaultMQPushConsumer start(RocketMqProperties properties, String group, String topic, String tag,
            ConsumeFromWhere fromWhere, MessageListenerConcurrently listener) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.setNamesrvAddr(properties.getNameServer());
        consumer.setConsumeFromWhere(fromWhere);
        consumer.setConsumeThreadMin(properties.getConsumeThreads());
        consumer.setConsumeThreadMax(properties.getConsumeThreads());
        consumer.setMaxReconsumeTimes(properties.getMaxReconsumeTimes());
        consumer.subscribe(topic, tag);
        consumer.registerMessageListener(listener);
        consumer.start();
        return consumer;
    }
}
