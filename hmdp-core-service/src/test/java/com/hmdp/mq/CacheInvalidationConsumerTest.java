package com.hmdp.mq;

import com.hmdp.mq.config.CacheMqProperties;
import com.hmdp.mq.consumer.CacheInvalidationConsumer;
import com.hmdp.mq.message.CacheInvalidationMessage;
import com.hmdp.cache.ShopTypeCacheService;
import com.hmdp.cache.VoucherListCacheService;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CacheInvalidationConsumerTest {

    private static final long DELIVERY_TAG = 42L;

    private final ShopTypeCacheService types = mock(ShopTypeCacheService.class);
    private final VoucherListCacheService vouchers = mock(VoucherListCacheService.class);
    private final Channel channel = mock(Channel.class);
    private final CacheMqProperties properties = new CacheMqProperties();
    private CacheInvalidationConsumer consumer;

    @BeforeEach
    void setUp() {
        properties.setRetryInterval(Duration.ofMillis(1));
        // RetryTemplate 在构造时读取重试间隔，必须在调整配置后创建消费者。
        consumer = new CacheInvalidationConsumer(types, vouchers, properties);
    }

    @Test
    void shouldInvalidateOnlyVoucherLocalCacheThenAck() throws Exception {
        consumer.onMessage(CacheInvalidationMessage.voucherList(7L), channel, DELIVERY_TAG);
        var order = inOrder(vouchers, channel);
        order.verify(vouchers).invalidateLocalCache(7L);
        order.verify(channel).basicAck(DELIVERY_TAG, false);
        verifyNoInteractions(types);
    }

    @Test
    void shouldInvalidateShopTypeLocalCache() throws Exception {
        consumer.onMessage(CacheInvalidationMessage.shopType(), channel, DELIVERY_TAG);
        verify(types).invalidateLocalCache();
        verify(channel).basicAck(DELIVERY_TAG, false);
        verifyNoInteractions(vouchers);
    }

    @Test
    void shouldRetryAndAckAfterRecovery() throws Exception {
        doThrow(new IllegalStateException("temporary"), new IllegalStateException("temporary"))
                .doNothing().when(vouchers).invalidateLocalCache(7L);
        consumer.onMessage(CacheInvalidationMessage.voucherList(7L), channel, DELIVERY_TAG);
        verify(vouchers, times(3)).invalidateLocalCache(7L);
        verify(channel).basicAck(DELIVERY_TAG, false);
        verify(channel, never()).basicReject(anyLong(), anyBoolean());
    }

    @Test
    void shouldRejectWithoutRequeueAfterRetriesToRouteToDlx() throws Exception {
        doThrow(new IllegalStateException("failed")).when(types).invalidateLocalCache();
        consumer.onMessage(CacheInvalidationMessage.shopType(), channel, DELIVERY_TAG);
        verify(types, times(3)).invalidateLocalCache();
        verify(channel).basicReject(DELIVERY_TAG, false);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
    }

    @Test
    void shouldDeadLetterInvalidMessage() throws Exception {
        consumer.onMessage(new CacheInvalidationMessage("UNKNOWN", null), channel, DELIVERY_TAG);
        verify(channel).basicReject(DELIVERY_TAG, false);
        verifyNoInteractions(types, vouchers);
    }

    @Test
    void shouldSafelyProcessDuplicateInvalidations() throws Exception {
        CacheInvalidationMessage message = CacheInvalidationMessage.voucherList(7L);
        consumer.onMessage(message, channel, DELIVERY_TAG);
        consumer.onMessage(message, channel, DELIVERY_TAG);
        verify(vouchers, times(2)).invalidateLocalCache(7L);
        verify(channel, times(2)).basicAck(DELIVERY_TAG, false);
    }

    @Test
    void shouldPropagateAckFailureWithoutRetryingOrRejectingOnSameChannel() throws Exception {
        doThrow(new IOException("connection lost")).when(channel).basicAck(DELIVERY_TAG, false);
        assertThrows(IOException.class,
                () -> consumer.onMessage(CacheInvalidationMessage.shopType(), channel, DELIVERY_TAG));
        verify(types).invalidateLocalCache();
        verify(channel, never()).basicReject(anyLong(), anyBoolean());
    }

    @Test
    void shouldRequeueIfRetryIsInterrupted() throws Exception {
        doThrow(new IllegalStateException("failed")).when(types).invalidateLocalCache();
        Thread.currentThread().interrupt();
        try {
            consumer.onMessage(CacheInvalidationMessage.shopType(), channel, DELIVERY_TAG);
            assertTrue(Thread.currentThread().isInterrupted());
            verify(channel).basicNack(DELIVERY_TAG, false, true);
        } finally {
            Thread.interrupted();
        }
    }
}
