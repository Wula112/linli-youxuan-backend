package com.hmdp.mq.message;

import org.springframework.util.Assert;

/** 只通知核查订单，状态和支付截止时间以数据库为准。 */
public record OrderTimeoutMessage(Long orderId) {

    public OrderTimeoutMessage {
        Assert.isTrue(orderId != null && orderId > 0, "orderId必须大于0");
    }
}
