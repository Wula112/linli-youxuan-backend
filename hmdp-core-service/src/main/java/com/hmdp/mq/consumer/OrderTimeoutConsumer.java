package com.hmdp.mq.consumer;

import com.hmdp.mq.message.OrderTimeoutMessage;
import com.hmdp.mq.support.MqMessageCodec;
import com.hmdp.service.IVoucherOrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import java.util.List;

/** 消费延迟关单消息；关单本身是条件更新，重复或提前投递都不会误关。 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderTimeoutConsumer implements MessageListenerConcurrently {

    private final IVoucherOrderService voucherOrderService;

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
        for (MessageExt message : messages) {
            Long orderId;
            try {
                orderId = MqMessageCodec.decode(message.getBody(), OrderTimeoutMessage.class).orderId();
            } catch (Exception ex) {
                log.error("关单消息无法解析，已跳过，msgId={}", message.getMsgId(), ex);
                continue;
            }
            try {
                voucherOrderService.closeExpiredOrder(orderId);
            } catch (Exception ex) {
                log.error("延迟关单失败，等待重投，orderId={}，已重投次数={}",
                        orderId, message.getReconsumeTimes(), ex);
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
