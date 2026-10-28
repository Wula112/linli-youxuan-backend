package com.hmdp.mq.config;

import org.springframework.amqp.core.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

/**
 * 缓存失效广播拓扑：fanout、实例独立队列及死信路由。
 */
@Configuration
@ConditionalOnProperty(prefix = "hmdp.cache.mq", name = "enabled", havingValue = "true", matchIfMissing = true)
public class CacheMqConfig {

    @Bean
    public Queue cacheInvalidationQueue(CacheMqProperties properties) {
        // 每个进程独立队列，实现广播；停用一天后回收，重启时本地缓存本来就是空的。
        return QueueBuilder.durable(properties.getQueuePrefix() + "." + UUID.randomUUID())
                .withArgument("x-expires", properties.getQueueExpires().toMillis())
                .deadLetterExchange(properties.getDeadLetterExchange())
                .deadLetterRoutingKey("failed").build();
    }

    @Bean
    public Declarables cacheMqDeclarations(CacheMqProperties properties,
                                           @Qualifier("cacheInvalidationQueue") Queue instanceQueue) {
        FanoutExchange exchange = new FanoutExchange(properties.getExchange(), true, false);
        DirectExchange deadExchange = new DirectExchange(properties.getDeadLetterExchange(), true, false);
        Queue deadQueue = QueueBuilder.durable(properties.getDeadLetterQueue()).build();
        return new Declarables(exchange, deadExchange, deadQueue,
                BindingBuilder.bind(instanceQueue).to(exchange),
                BindingBuilder.bind(deadQueue).to(deadExchange).with("failed"));
    }
}
