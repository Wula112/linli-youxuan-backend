package com.hmdp.common.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/** 沿用订单表已有状态值；状态变更由数据库条件更新保证，避免无条件覆盖终态。 */
@Getter
@RequiredArgsConstructor
public enum VoucherOrderStatus {

    UNPAID(1, "未支付"),
    PAID(2, "已支付"),
    USED(3, "已核销"),
    CLOSED(4, "已取消"),
    REFUNDING(5, "退款中"),
    REFUNDED(6, "已退款");

    private final int code;
    private final String description;
}
