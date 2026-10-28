package com.hmdp.common.constant;

public class RedisConstants {
    public static final String RATE_LIMIT_KEY = "rate:limit:";
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final Long LOGIN_CODE_TTL = 2L;
    public static final String LOGIN_USER_KEY = "login:token:";
    public static final Long LOGIN_USER_TTL = 36000L;

    public static final Long CACHE_NULL_TTL = 2L;

    public static final Long CACHE_SHOP_TTL = 30L;
    public static final String CACHE_SHOP_KEY = "cache:shop:";
    public static final String CACHE_SHOP_TYPE_KEY = "cache:shop-type";

    public static final String CACHE_SHOP_VOUCHER_KEY = "cache:shop:voucher:";
    // JSON商铺ID列表：只在整轮Redis预热成功后发布，供各实例装载L1。
    public static final String CACHE_WARMUP_SHOP_IDS_KEY = "cache:warmup:shop-ids";

    // 延迟双删的间隔，单位：毫秒
    public static final Long CACHE_SHOP_DELETE_DELAY = 1000L;

    public static final String BLOOM_SHOP_KEY = "bloom:shop";
    public static final String BLOOM_SHOP_READY_KEY = "bloom:shop:ready";
    public static final Long BLOOM_SHOP_EXPECTED_INSERTIONS = 1000000L;
    public static final double BLOOM_SHOP_FALSE_PROBABILITY = 0.01;

    public static final String LOCK_SHOP_KEY = "lock:shop:";
    public static final Long LOCK_SHOP_TTL = 10L;

    public static final String SECKILL_STOCK_KEY = "seckill:stock:";
    // 旧版Set Key，保留给历史代码对照；当前使用reservation Hash校验一人一单。
    public static final String SECKILL_ORDER_KEY = "seckill:order:";
    public static final String SECKILL_RESERVATION_KEY = "seckill:reservation:";
    public static final String SECKILL_PENDING_KEY = "seckill:pending";
    // Hash：完整pending member -> 已使用的对账补发轮次，不随消息重建而归零。
    public static final String SECKILL_PENDING_RETRIES_KEY = "seckill:pending:retries";
    public static final String LOCK_VOUCHER_ORDER_KEY = "lock:order:";
    public static final String BLOG_LIKED_KEY = "blog:liked:";
    public static final String FEED_KEY = "feed:";
    public static final String SHOP_GEO_KEY = "shop:geo:";
    public static final String USER_SIGN_KEY = "sign:";
}
