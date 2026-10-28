package com.hmdp.mq.consumer;

import com.hmdp.mq.config.OrderTimeoutMqConfig;
import com.hmdp.mq.message.OrderTimeoutMessage;
import com.hmdp.service.IVoucherOrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/** 默认AUTO确认，异常由既有容器配置有限重试，耗尽进入关单死信队列。 */
@Component
@RequiredArgsConstructor
public class OrderTimeoutConsumer {

    private final IVoucherOrderService voucherOrderService;

    @RabbitListener(queues = OrderTimeoutMqConfig.CLOSE_QUEUE)
    public void consume(OrderTimeoutMessage message) {
        voucherOrderService.closeExpiredOrder(message.orderId());
    }
}
