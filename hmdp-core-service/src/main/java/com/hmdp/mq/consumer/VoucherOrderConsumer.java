package com.hmdp.mq.consumer;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.mq.config.VoucherOrderMqConfig;
import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.service.IVoucherOrderService;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import static com.hmdp.common.constant.RedisConstants.LOCK_VOUCHER_ORDER_KEY;

/** 使用默认 AUTO 确认；事务方法成功返回后容器才 ACK，异常由容器有限重试并进入死信。 */
@Component
@RequiredArgsConstructor
public class VoucherOrderConsumer {

    private final IVoucherOrderService voucherOrderService;
    private final RedissonClient redissonClient;

    @RabbitListener(queues = VoucherOrderMqConfig.QUEUE)
    public void consume(VoucherOrderMessage message) {
        RLock lock = redissonClient.getLock(LOCK_VOUCHER_ORDER_KEY + message.voucherId() + ":" + message.userId());
        if (!lock.tryLock()) {
            throw new IllegalStateException("订单正在处理，稍后重试，orderId=" + message.orderId());
        }
        try {
            VoucherOrder order = new VoucherOrder().setId(message.orderId())
                    .setUserId(message.userId()).setVoucherId(message.voucherId());
            // 调用注入的代理，确保数据库提交完成后才释放订单锁。
            voucherOrderService.createVoucherOrder(order);
        } finally {
            lock.unlock();
        }
    }
}
