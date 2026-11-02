package com.hmdp.mq.consumer;

import com.hmdp.cache.ShopTypeCacheService;
import com.hmdp.cache.VoucherListCacheService;
import com.hmdp.mq.message.CacheInvalidationMessage;
import com.hmdp.mq.support.MqMessageCodec;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 消费失效广播，只清理当前实例的本地缓存。
 *
 * <p>消息体只带缓存类型和业务标识；同一实例重复消费是幂等的。
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "hmdp.cache.mq", name = "enabled", havingValue = "true", matchIfMissing = true)
public class CacheInvalidationConsumer implements MessageListenerConcurrently {

    private final ShopTypeCacheService shopTypeCacheService;
    private final VoucherListCacheService voucherListCacheService;

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
        for (MessageExt message : messages) {
            CacheInvalidationMessage event;
            try {
                event = MqMessageCodec.decode(message.getBody(), CacheInvalidationMessage.class);
            } catch (Exception ex) {
                log.error("缓存失效消息无法解析，已跳过，msgId={}", message.getMsgId(), ex);
                continue;
            }
            try {
                invalidateLocalCache(event);
            } catch (Exception ex) {
                log.error("清理本地缓存失败，等待重投，event={}，已重投次数={}",
                        event, message.getReconsumeTimes(), ex);
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    private void invalidateLocalCache(CacheInvalidationMessage event) {
        // 校验缓存类型和业务标识，非法消息同样走重投直到进入死信队列
        event.redisKey();
        if (CacheInvalidationMessage.SHOP_TYPE.equals(event.getCacheName())) {
            shopTypeCacheService.invalidateLocalCache();
        } else {
            voucherListCacheService.invalidateLocalCache(event.getShopId());
        }
    }
}
