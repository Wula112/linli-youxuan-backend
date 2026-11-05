package com.hmdp.mq.consumer;

import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.mq.support.MqMessageCodec;
import com.hmdp.service.impl.VoucherOrderProcessService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import java.util.List;

/** 死信成功登记后才确认；处理失败仍交给 RocketMQ 重试。 */
@Slf4j
@Component
@RequiredArgsConstructor
public class VoucherOrderDeadLetterConsumer implements MessageListenerConcurrently {
    private final VoucherOrderProcessService processService;

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
        for (MessageExt message : messages) {
            try {
                VoucherOrderMessage event = MqMessageCodec.decode(message.getBody(), VoucherOrderMessage.class);
                processService.recordDeadLetter(event);
                log.warn("订单死信已核查，orderId={}", event.orderId());
            } catch (Exception ex) {
                log.error("订单死信登记失败，等待重试，msgId={}", message.getMsgId(), ex);
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
