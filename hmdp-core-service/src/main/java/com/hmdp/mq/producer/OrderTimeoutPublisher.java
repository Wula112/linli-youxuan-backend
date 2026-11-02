package com.hmdp.mq.producer;

import com.hmdp.config.VoucherOrderProperties;
import com.hmdp.mq.config.RocketMqConstants;
import com.hmdp.mq.message.OrderTimeoutMessage;
import com.hmdp.mq.support.MqMessageCodec;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * 提交后发送延迟关单消息，失败由数据库扫描兜底。
 *
 * <p>使用 5.x 定时消息按支付超时精确投递；即使提前或重复投递，关单也是带截止时间的条件更新。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderTimeoutPublisher {

    private final TransactionMQProducer producer;
    private final VoucherOrderProperties orderProperties;

    public void publishAfterCommit(Long orderId) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()
                || !TransactionSynchronizationManager.isSynchronizationActive()) {
            throw new IllegalStateException("必须在建单事务中注册延迟关单消息");
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                try {
                    publish(orderId);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    log.error("延迟关单消息发送被中断，等待数据库扫描兜底，orderId={}", orderId, ex);
                } catch (Exception ex) {
                    // 订单已经提交，不能因通知失败回滚订单或恢复预占
                    log.error("延迟关单消息发送失败，等待数据库扫描兜底，orderId={}", orderId, ex);
                }
            }
        });
    }

    public void publish(Long orderId)
            throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        Message message = new Message(RocketMqConstants.ORDER_TOPIC, RocketMqConstants.ORDER_TIMEOUT_TAG,
                MqMessageCodec.encode(new OrderTimeoutMessage(orderId)));
        message.setDelayTimeSec(orderProperties.getPayTimeout().toSeconds());
        SendResult result = producer.send(message);
        if (result.getSendStatus() != SendStatus.SEND_OK) {
            throw new IllegalStateException("延迟关单消息发送失败，orderId=" + orderId
                    + "，状态=" + result.getSendStatus());
        }
    }
}
