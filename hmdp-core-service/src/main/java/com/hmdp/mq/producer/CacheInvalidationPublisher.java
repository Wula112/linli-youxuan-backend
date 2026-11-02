package com.hmdp.mq.producer;

import com.hmdp.mq.config.RocketMqConstants;
import com.hmdp.mq.message.CacheInvalidationMessage;
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
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * 在业务事务提交后删除 Redis 缓存，再广播本地缓存失效。
 *
 * <p>先删 Redis 再发消息：删除是幂等的，重试不会产生副作用；发送失败只记录，不影响已提交的业务。
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "hmdp.cache.mq", name = "enabled", havingValue = "true", matchIfMissing = true)
public class CacheInvalidationPublisher {

    private final StringRedisTemplate stringRedisTemplate;
    private final TransactionMQProducer producer;

    // 事务回滚不会执行；无事务时，数据库操作已由调用方完成，直接执行失效。
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT, fallbackExecution = true)
    public void onCacheChanged(CacheInvalidationMessage event) {
        try {
            stringRedisTemplate.delete(event.redisKey());
        } catch (Exception ex) {
            // Redis 删除失败也继续广播，尽可能清除各实例本地缓存
            log.error("Redis缓存删除失败，key={}", event.redisKey(), ex);
        }
        try {
            publish(event);
        } catch (Exception ex) {
            // 数据库已经提交，此处只记录失败，不能再回滚业务事务
            log.error("缓存失效消息发送失败，需要补发，event={}", event, ex);
        }
    }

    private void publish(CacheInvalidationMessage event)
            throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        SendResult result = producer.send(new Message(RocketMqConstants.CACHE_TOPIC,
                RocketMqConstants.CACHE_TAG, MqMessageCodec.encode(event)));
        if (result.getSendStatus() != SendStatus.SEND_OK) {
            throw new IllegalStateException("缓存失效消息发送失败，状态=" + result.getSendStatus());
        }
    }
}
