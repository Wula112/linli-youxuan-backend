package com.hmdp.mq.consumer;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.mq.support.MqMessageCodec;
import com.hmdp.service.IVoucherOrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.hmdp.common.constant.RedisConstants.LOCK_VOUCHER_ORDER_KEY;

/** 消费下单消息：持锁建单，失败交给 Broker 重投，重投耗尽进入死信队列。 */
@Slf4j
@Component
@RequiredArgsConstructor
public class VoucherOrderConsumer implements MessageListenerConcurrently {

    private final IVoucherOrderService voucherOrderService;
    private final RedissonClient redissonClient;

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
        for (MessageExt message : messages) {
            VoucherOrderMessage payload;
            try {
                payload = MqMessageCodec.decode(message.getBody(), VoucherOrderMessage.class);
            } catch (Exception ex) {
                // 消息体损坏时重投也不会成功，记录后跳过
                log.error("订单消息无法解析，已跳过，msgId={}", message.getMsgId(), ex);
                continue;
            }
            try {
                createOrder(payload);
            } catch (Exception ex) {
                log.error("秒杀订单消费失败，等待重投，orderId={}，已重投次数={}",
                        payload.orderId(), message.getReconsumeTimes(), ex);
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    private void createOrder(VoucherOrderMessage message) {
        RLock lock = redissonClient.getLock(LOCK_VOUCHER_ORDER_KEY + message.voucherId() + ":" + message.userId());
        if (!lock.tryLock()) {
            throw new IllegalStateException("订单正在处理，稍后重试，orderId=" + message.orderId());
        }
        try {
            VoucherOrder order = new VoucherOrder().setId(message.orderId())
                    .setUserId(message.userId()).setVoucherId(message.voucherId());
            // 调用注入的代理，确保数据库提交完成后才释放订单锁
            voucherOrderService.createVoucherOrder(order);
        } finally {
            lock.unlock();
        }
    }
}
