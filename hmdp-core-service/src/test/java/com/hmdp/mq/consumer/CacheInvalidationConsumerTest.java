package com.hmdp.mq.consumer;

import com.hmdp.cache.ShopTypeCacheService;
import com.hmdp.cache.VoucherListCacheService;
import com.hmdp.mq.message.CacheInvalidationMessage;
import com.hmdp.mq.support.MqMessageCodec;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class CacheInvalidationConsumerTest {

    private final ShopTypeCacheService types = mock(ShopTypeCacheService.class);
    private final VoucherListCacheService vouchers = mock(VoucherListCacheService.class);
    private final CacheInvalidationConsumer consumer = new CacheInvalidationConsumer(types, vouchers);

    @Test
    void shouldInvalidateOnlyVoucherLocalCache() {
        assertEquals(ConsumeConcurrentlyStatus.CONSUME_SUCCESS,
                consumer.consumeMessage(List.of(message(CacheInvalidationMessage.voucherList(7L))), null));
        verify(vouchers).invalidateLocalCache(7L);
        verifyNoInteractions(types);
    }

    @Test
    void shouldInvalidateShopTypeLocalCache() {
        assertEquals(ConsumeConcurrentlyStatus.CONSUME_SUCCESS,
                consumer.consumeMessage(List.of(message(CacheInvalidationMessage.shopType())), null));
        verify(types).invalidateLocalCache();
        verifyNoInteractions(vouchers);
    }

    @Test
    void shouldAskForRedeliveryWhenCleanupFails() {
        doThrow(new IllegalStateException("failed")).when(types).invalidateLocalCache();
        assertEquals(ConsumeConcurrentlyStatus.RECONSUME_LATER,
                consumer.consumeMessage(List.of(message(CacheInvalidationMessage.shopType())), null));
    }

    @Test
    void shouldAskForRedeliveryForUnknownCacheType() {
        assertEquals(ConsumeConcurrentlyStatus.RECONSUME_LATER,
                consumer.consumeMessage(List.of(message(new CacheInvalidationMessage("UNKNOWN", null))), null));
        verifyNoInteractions(types, vouchers);
    }

    @Test
    void shouldSafelyProcessDuplicateInvalidations() {
        MessageExt message = message(CacheInvalidationMessage.voucherList(7L));
        assertEquals(ConsumeConcurrentlyStatus.CONSUME_SUCCESS, consumer.consumeMessage(List.of(message), null));
        assertEquals(ConsumeConcurrentlyStatus.CONSUME_SUCCESS, consumer.consumeMessage(List.of(message), null));
        verify(vouchers, times(2)).invalidateLocalCache(7L);
    }

    @Test
    void shouldSkipUnreadableBodyInsteadOfLooping() {
        MessageExt broken = new MessageExt();
        broken.setBody(new byte[]{'x'});
        assertEquals(ConsumeConcurrentlyStatus.CONSUME_SUCCESS, consumer.consumeMessage(List.of(broken), null));
        verifyNoInteractions(types, vouchers);
    }

    private MessageExt message(CacheInvalidationMessage event) {
        MessageExt message = new MessageExt();
        message.setBody(MqMessageCodec.encode(event));
        message.setMsgId("msg-1");
        return message;
    }
}
