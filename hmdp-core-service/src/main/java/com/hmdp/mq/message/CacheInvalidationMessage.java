package com.hmdp.mq.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.util.Assert;

import static com.hmdp.common.constant.RedisConstants.CACHE_SHOP_TYPE_KEY;
import static com.hmdp.common.constant.RedisConstants.CACHE_SHOP_VOUCHER_KEY;

/** 缓存失效通知，只携带缓存类型和业务标识，不传输缓存数据。 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CacheInvalidationMessage {

    public static final String SHOP_TYPE = "SHOP_TYPE";
    public static final String VOUCHER_LIST = "VOUCHER_LIST";

    private String cacheName;
    private Long shopId;

    public static CacheInvalidationMessage shopType() {
        return new CacheInvalidationMessage(SHOP_TYPE, null);
    }

    public static CacheInvalidationMessage voucherList(Long shopId) {
        CacheInvalidationMessage message = new CacheInvalidationMessage(VOUCHER_LIST, shopId);
        message.redisKey();
        return message;
    }

    public String redisKey() {
        if (SHOP_TYPE.equals(cacheName)) {
            return CACHE_SHOP_TYPE_KEY;
        }
        Assert.isTrue(VOUCHER_LIST.equals(cacheName), "未知的缓存类型：" + cacheName);
        Assert.isTrue(shopId != null && shopId > 0, "优惠券列表失效消息的shopId必须大于0");
        return CACHE_SHOP_VOUCHER_KEY + shopId;
    }
}
