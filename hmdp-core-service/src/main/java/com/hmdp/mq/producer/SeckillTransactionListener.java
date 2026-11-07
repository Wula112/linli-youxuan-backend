package com.hmdp.mq.producer;

import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.mq.support.MqMessageCodec;
import com.hmdp.repository.redis.SeckillReservation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

/**
 * 秒杀下单的本地事务：半消息落盘后执行 Lua 预扣，成功提交、失败回滚。
 *
 * <p>本地事务没给出结果时返回 UNKNOW，由 Broker 回查事务标记决定半消息去留。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SeckillTransactionListener implements TransactionListener {

    private final SeckillReservation seckillReservation;

    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object argument) {
        SeckillTxContext context = (SeckillTxContext) argument;
        VoucherOrderMessage order = context.getMessage();
        try {
            long result = seckillReservation.reserve(order.voucherId(), order.userId(), order.orderId());
            context.complete(result);
            if (result == SeckillReservation.SUCCESS) {
                return LocalTransactionState.COMMIT_MESSAGE;
            }
            // 库存不足、重复下单或熔断：半消息丢弃，不会投递给消费者。
            return LocalTransactionState.ROLLBACK_MESSAGE;
        } catch (Exception ex) {
            log.error("秒杀本地事务执行异常，等待Broker回查事务标记，orderId={}", order.orderId(), ex);
            context.complete(SeckillReservation.UNKNOWN);
            return LocalTransactionState.UNKNOW;
        }
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt message) {
        try {
            VoucherOrderMessage order = MqMessageCodec.decode(message.getBody(), VoucherOrderMessage.class);
            boolean reserved = seckillReservation.hasTransactionMarker(order.orderId());
            log.info("事务回查：orderId={}，事务标记存在={}", order.orderId(), reserved);
            return reserved ? LocalTransactionState.COMMIT_MESSAGE : LocalTransactionState.ROLLBACK_MESSAGE;
        } catch (Exception ex) {
            log.error("事务回查失败，等待下一次回查，msgId={}", message.getMsgId(), ex);
            return LocalTransactionState.UNKNOW;
        }
    }
}
