package com.hmdp.mq;

import com.hmdp.framework.mq.config.RabbitConfig;
import com.hmdp.mq.config.CacheMqConfig;
import com.hmdp.mq.config.CacheMqProperties;
import com.hmdp.mq.config.VoucherOrderMqConfig;

import com.hmdp.mq.config.*;
import com.hmdp.mq.message.VoucherOrderMessage;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.boot.amqp.autoconfigure.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.junit.jupiter.api.Assertions.*;

class RabbitConfigTest {

    private final ApplicationContextRunner context = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(RabbitAutoConfiguration.class))
            .withUserConfiguration(RabbitConfig.class, CacheMqConfig.class,
                    CacheMqProperties.class, VoucherOrderMqConfig.class)
            .withPropertyValues("spring.rabbitmq.publisher-confirm-type=correlated",
                    "spring.rabbitmq.publisher-returns=true",
                    "spring.rabbitmq.listener.simple.acknowledge-mode=auto",
                    "spring.rabbitmq.listener.simple.retry.enabled=true",
                    "spring.rabbitmq.listener.simple.retry.max-retries=2");

    @Test
    void shouldShareOneTemplateAndSeparateAckStrategies() {
        context.run(ctx -> {
            assertNull(ctx.getStartupFailure());
            assertEquals(1, ctx.getBeansOfType(RabbitTemplate.class).size());
            RabbitTemplate template = ctx.getBean(RabbitTemplate.class);
            assertSame(ctx.getBean(JacksonJsonMessageConverter.class), template.getMessageConverter());
            var auto = ctx.getBean("rabbitListenerContainerFactory", SimpleRabbitListenerContainerFactory.class);
            var manual = ctx.getBean("manualAckFactory", SimpleRabbitListenerContainerFactory.class);
            assertEquals(AcknowledgeMode.AUTO, auto.createListenerContainer().getAcknowledgeMode());
            assertEquals(AcknowledgeMode.MANUAL, manual.createListenerContainer().getAcknowledgeMode());
        });
    }

    @Test
    void shouldKeepOrdersAvailableWhenCacheBroadcastIsDisabled() {
        context.withPropertyValues("hmdp.cache.mq.enabled=false").run(ctx -> {
            assertNull(ctx.getStartupFailure());
            assertTrue(ctx.containsBean("rabbitTemplate"));
            assertTrue(ctx.containsBean("voucherOrderQueue"));
            assertFalse(ctx.containsBean("cacheInvalidationQueue"));
        });
    }

    @Test
    void shouldDefineDirectOrderRouteAndDeadLetterRoute() {
        VoucherOrderMqConfig config = new VoucherOrderMqConfig();
        Queue queue = config.voucherOrderQueue();
        assertTrue(queue.isDurable());
        assertFalse(queue.isExclusive());
        assertEquals(VoucherOrderMqConfig.DEAD_EXCHANGE, queue.getArguments().get("x-dead-letter-exchange"));
        Binding binding = config.voucherOrderBinding(queue, config.voucherOrderExchange());
        assertEquals(VoucherOrderMqConfig.ROUTING_KEY, binding.getRoutingKey());
        assertEquals(VoucherOrderMqConfig.QUEUE, binding.getDestination());
        assertTrue(config.voucherOrderDeadLetters().getDeclarables().stream().anyMatch(d ->
                d instanceof Binding b && VoucherOrderMqConfig.DEAD_QUEUE.equals(b.getDestination())
                        && VoucherOrderMqConfig.DEAD_ROUTING_KEY.equals(b.getRoutingKey())));
    }

    @Test
    void shouldRoundTripOrderAsPersistentJson() {
        var converter = new RabbitConfig().messageConverter();
        VoucherOrderMessage dto = new VoucherOrderMessage(1000L, 5L, 10L);
        Message message = converter.toMessage(dto, new MessageProperties());
        assertEquals(MessageDeliveryMode.PERSISTENT, message.getMessageProperties().getDeliveryMode());
        assertNotNull(message.getMessageProperties().getMessageId());
        assertEquals(dto, converter.fromMessage(message));
    }
}
