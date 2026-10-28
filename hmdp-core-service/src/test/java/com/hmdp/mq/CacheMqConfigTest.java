package com.hmdp.mq;

import com.hmdp.mq.config.CacheMqConfig;
import com.hmdp.mq.config.CacheMqProperties;
import com.hmdp.framework.mq.config.RabbitConfig;
import com.hmdp.mq.message.CacheInvalidationMessage;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.boot.amqp.autoconfigure.RabbitTemplateConfigurer;
import org.springframework.boot.amqp.autoconfigure.SimpleRabbitListenerContainerFactoryConfigurer;

import java.nio.charset.StandardCharsets;

import static com.hmdp.common.constant.RedisConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CacheMqConfigTest {

    @Test
    void shouldUseDistinctDurableInstanceQueuesWithDlxAndExpiry() {
        CacheMqConfig config = new CacheMqConfig();
        CacheMqProperties properties = new CacheMqProperties();
        Queue first = config.cacheInvalidationQueue(properties);
        Queue second = config.cacheInvalidationQueue(properties);
        assertNotEquals(first.getName(), second.getName());
        assertTrue(first.isDurable());
        assertEquals(properties.getDeadLetterExchange(), first.getArguments().get("x-dead-letter-exchange"));
        assertEquals("failed", first.getArguments().get("x-dead-letter-routing-key"));
        assertEquals(properties.getQueueExpires().toMillis(), first.getArguments().get("x-expires"));
        var declarations = config.cacheMqDeclarations(properties, first).getDeclarables();
        assertTrue(declarations.stream().anyMatch(d -> d instanceof FanoutExchange exchange
                && properties.getExchange().equals(exchange.getName())));
        assertTrue(declarations.stream().anyMatch(d -> d instanceof Binding binding
                && first.getName().equals(binding.getDestination())
                && properties.getExchange().equals(binding.getExchange())));
        assertTrue(declarations.stream().anyMatch(d -> d instanceof Binding binding
                && properties.getDeadLetterQueue().equals(binding.getDestination())
                && properties.getDeadLetterExchange().equals(binding.getExchange())
                && "failed".equals(binding.getRoutingKey())));
    }

    @Test
    void shouldConfigureJsonConverterAndMandatoryPublishing() {
        RabbitConfig config = new RabbitConfig();
        JacksonJsonMessageConverter converter = config.messageConverter();
        RabbitTemplateConfigurer configurer = new RabbitTemplateConfigurer(new RabbitProperties());
        configurer.setMessageConverter(converter);
        RabbitTemplate template = config.rabbitTemplate(mock(CachingConnectionFactory.class), configurer);
        assertSame(converter, template.getMessageConverter());
        assertTrue(template.isMandatoryFor(new Message(new byte[0])));
    }

    @Test
    void shouldSerializeMessageAsPersistentJsonWithMessageId() {
        JacksonJsonMessageConverter converter = new RabbitConfig().messageConverter();
        Message message = converter.toMessage(CacheInvalidationMessage.voucherList(7L), new MessageProperties());

        assertNotNull(message.getMessageProperties().getMessageId());
        assertEquals(MessageDeliveryMode.PERSISTENT, message.getMessageProperties().getDeliveryMode());
        assertEquals(MessageProperties.CONTENT_TYPE_JSON, message.getMessageProperties().getContentType());
        String json = new String(message.getBody(), StandardCharsets.UTF_8);
        assertTrue(json.contains("\"cacheName\":\"VOUCHER_LIST\""));
        assertTrue(json.contains("\"shopId\":7"));
    }

    @Test
    void shouldDeserializeMessageWithTrustedPackage() {
        JacksonJsonMessageConverter converter = new RabbitConfig().messageConverter();
        Message message = converter.toMessage(CacheInvalidationMessage.voucherList(7L), new MessageProperties());

        CacheInvalidationMessage result = (CacheInvalidationMessage) converter.fromMessage(message);

        assertEquals(CacheInvalidationMessage.VOUCHER_LIST, result.getCacheName());
        assertEquals(7L, result.getShopId());
    }

    @Test
    void shouldRequireManualAck() {
        RabbitConfig config = new RabbitConfig();
        var configurer = new SimpleRabbitListenerContainerFactoryConfigurer(new RabbitProperties());
        var factory = config.manualAckFactory(mock(CachingConnectionFactory.class), configurer);
        var container = factory.createListenerContainer();
        assertEquals(AcknowledgeMode.MANUAL, container.getAcknowledgeMode());
    }

    @Test
    void shouldValidateMessageBeforeDeletingAnything() {
        assertEquals(CACHE_SHOP_TYPE_KEY, CacheInvalidationMessage.shopType().redisKey());
        assertEquals(CACHE_SHOP_VOUCHER_KEY + 7, CacheInvalidationMessage.voucherList(7L).redisKey());
        assertThrows(IllegalArgumentException.class, () -> CacheInvalidationMessage.voucherList(null));
        assertThrows(IllegalArgumentException.class, () -> CacheInvalidationMessage.voucherList(-1L));
        assertThrows(IllegalArgumentException.class, () -> new CacheInvalidationMessage("unknown", 7L).redisKey());
    }
}
