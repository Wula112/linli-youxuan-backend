package com.hmdp.mq.config;

/** 主题、标签与消费者组：生产者与消费者共用一处声明，避免字符串散落。 */
public final class RocketMqConstants {

    /** 秒杀订单主题：下单消息与延迟关单消息共用，用标签区分。 */
    public static final String ORDER_TOPIC = "hmdp-voucher-order";
    public static final String ORDER_CREATE_TAG = "create";
    public static final String ORDER_TIMEOUT_TAG = "timeout";
    public static final String ORDER_CONSUMER_GROUP = "hmdp-voucher-order-consumer";
    public static final String ORDER_DEAD_LETTER_TOPIC = "%DLQ%" + ORDER_CONSUMER_GROUP;
    public static final String ORDER_DEAD_LETTER_CONSUMER_GROUP = "hmdp-voucher-order-dead-letter-consumer";
    public static final String TIMEOUT_CONSUMER_GROUP = "hmdp-order-timeout-consumer";

    /** 缓存失效主题：实例内队列语义由“每实例一个消费者组”实现广播。 */
    public static final String CACHE_TOPIC = "hmdp-cache-invalidation";
    public static final String CACHE_TAG = "invalidate";
    public static final String CACHE_CONSUMER_GROUP_PREFIX = "hmdp-cache-invalidation-";

    private RocketMqConstants() {
    }
}
