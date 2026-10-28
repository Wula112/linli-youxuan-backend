package com.hmdp.mq.config;

import com.hmdp.config.VoucherOrderProperties;
import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 固定消息TTL + DLX触发关单；延迟队列不配置消费者。 */
@Configuration
public class OrderTimeoutMqConfig {

    public static final String DELAY_EXCHANGE = "order.delay.exchange";
    public static final String DELAY_QUEUE = "order.delay.queue";
    public static final String CLOSE_EXCHANGE = "order.close.exchange";
    public static final String CLOSE_QUEUE = "order.close.queue";
    public static final String ROUTING_KEY = "order.close";
    public static final String DEAD_EXCHANGE = "order.close.dlx";
    public static final String DEAD_QUEUE = "order.close.dlq";
    public static final String DEAD_ROUTING_KEY = "order.close.failed";

    @Bean
    public Declarables orderTimeoutDeclarables(VoucherOrderProperties properties) {
        DirectExchange delay = new DirectExchange(DELAY_EXCHANGE, true, false);
        DirectExchange close = new DirectExchange(CLOSE_EXCHANGE, true, false);
        DirectExchange dead = new DirectExchange(DEAD_EXCHANGE, true, false);
        Queue delayQueue = QueueBuilder.durable(DELAY_QUEUE)
                .withArgument("x-message-ttl", properties.getPayTimeout().toMillis())
                .deadLetterExchange(CLOSE_EXCHANGE).deadLetterRoutingKey(ROUTING_KEY).build();
        Queue closeQueue = QueueBuilder.durable(CLOSE_QUEUE)
                .deadLetterExchange(DEAD_EXCHANGE).deadLetterRoutingKey(DEAD_ROUTING_KEY).build();
        Queue deadQueue = QueueBuilder.durable(DEAD_QUEUE).build();
        return new Declarables(delay, close, dead, delayQueue, closeQueue, deadQueue,
                BindingBuilder.bind(delayQueue).to(delay).with(ROUTING_KEY),
                BindingBuilder.bind(closeQueue).to(close).with(ROUTING_KEY),
                BindingBuilder.bind(deadQueue).to(dead).with(DEAD_ROUTING_KEY));
    }
}
