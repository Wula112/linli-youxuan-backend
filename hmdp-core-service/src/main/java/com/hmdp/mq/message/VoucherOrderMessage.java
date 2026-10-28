package com.hmdp.mq.message;

import org.springframework.util.Assert;

/** 只传创建订单所需的标识，业务实体由消费端构造。 */
public record VoucherOrderMessage(Long orderId, Long userId, Long voucherId, int retryCount) {

    public VoucherOrderMessage(Long orderId, Long userId, Long voucherId) {
        this(orderId, userId, voucherId, 0);
    }

    public VoucherOrderMessage {
        Assert.isTrue(orderId != null && orderId > 0, "orderId必须大于0");
        Assert.isTrue(userId != null && userId > 0, "userId必须大于0");
        Assert.isTrue(voucherId != null && voucherId > 0, "voucherId必须大于0");
        Assert.isTrue(retryCount >= 0, "retryCount不能为负");
    }

    /** 全局待对账索引中保留完整定位信息，避免反向扫描预占Hash。 */
    public String pendingMember() {
        return voucherId + ":" + userId + ":" + orderId;
    }

    public static VoucherOrderMessage fromPendingMember(String member) {
        String[] parts = member.split(":", -1);
        Assert.isTrue(parts.length == 3, "待对账成员格式必须为voucherId:userId:orderId");
        return new VoucherOrderMessage(Long.valueOf(parts[2]), Long.valueOf(parts[1]), Long.valueOf(parts[0]));
    }

    public VoucherOrderMessage incrementRetry() {
        return new VoucherOrderMessage(orderId, userId, voucherId, retryCount + 1);
    }
}