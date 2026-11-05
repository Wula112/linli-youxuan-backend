package com.hmdp.common.enums;

/** 订单创建与预占释放共用的状态；终态不得重新进入建单流程。 */
public enum OrderProcessStatus {
    PENDING,
    CREATED,
    DEAD_LETTER,
    COMPENSATING,
    COMPENSATED
}
