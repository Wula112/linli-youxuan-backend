package com.hmdp.mq.config;

import org.springframework.amqp.core.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 秒杀订单使用共享队列竞争消费，与缓存广播的实例独立队列分开。 */
@Configuration
public class VoucherOrderMqConfig {

    public static final String EXCHANGE = "voucher.order.exchange";
    public static final String QUEUE = "voucher.order.queue";
    public static final String ROUTING_KEY = "voucher.order.create";
    public static final String DEAD_EXCHANGE = "voucher.order.dlx";
    public static final String DEAD_QUEUE = "voucher.order.dlq";
    public static final String DEAD_ROUTING_KEY = "voucher.order.failed";

    @Bean
    public DirectExchange voucherOrderExchange() {
        return new DirectExchange(EXCHANGE, true, false);
    }

    @Bean
    public Queue voucherOrderQueue() {
        return QueueBuilder.durable(QUEUE)
                .deadLetterExchange(DEAD_EXCHANGE).deadLetterRoutingKey(DEAD_ROUTING_KEY).build();
    }

    @Bean
    public Binding voucherOrderBinding(@Qualifier("voucherOrderQueue") Queue queue,
            @Qualifier("voucherOrderExchange") DirectExchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with(ROUTING_KEY);
    }

    @Bean
    public Declarables voucherOrderDeadLetters() {
        DirectExchange exchange = new DirectExchange(DEAD_EXCHANGE, true, false);
        Queue queue = QueueBuilder.durable(DEAD_QUEUE).build();
        return new Declarables(exchange, queue, BindingBuilder.bind(queue).to(exchange).with(DEAD_ROUTING_KEY));
    }
}
