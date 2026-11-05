package com.hmdp.mq.producer;

import com.hmdp.mq.config.RocketMqConstants;
import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.mq.support.MqMessageCodec;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.springframework.stereotype.Component;

/** 秒杀订单事务消息生产者；消费失败由 RocketMQ 重试。 */
@Slf4j
@Component
@RequiredArgsConstructor
public class VoucherOrderPublisher {

    private final TransactionMQProducer producer;

    /**
     * 发送下单事务消息：半消息先落盘，秒杀 Lua 由本地事务执行，成功才 COMMIT。
     *
     * <p>返回 Lua 返回码（0 成功、1 库存不足、2 重复下单、-1 未知）。提交结果丢失时由 Broker
     * 回查事务标记决定消息去留。
     */
    public long sendInTransaction(VoucherOrderMessage message) {
        SeckillTxContext context = new SeckillTxContext(message);
        try {
            producer.sendMessageInTransaction(createMessage(message), context);
        } catch (MQClientException ex) {
            // 发送或提交确认失败：本地事务已执行时以返回码为准，否则返回 UNKNOWN 由入口回退预扣
            log.warn("下单事务消息发送异常，orderId={}，本地事务返回码={}",
                    message.orderId(), context.getResult(), ex);
        }
        return context.getResult();
    }

    private Message createMessage(VoucherOrderMessage message) {
        return new Message(RocketMqConstants.ORDER_TOPIC, RocketMqConstants.ORDER_CREATE_TAG,
                MqMessageCodec.encode(message));
    }
}
