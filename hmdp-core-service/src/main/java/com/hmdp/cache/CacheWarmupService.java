package com.hmdp.cache;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.cache.config.CacheWarmupProperties;
import com.hmdp.entity.Shop;
import com.hmdp.framework.cache.RedisData;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.mapper.VoucherMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.common.constant.RedisConstants.CACHE_NULL_TTL;
import static com.hmdp.common.constant.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.common.constant.RedisConstants.CACHE_WARMUP_SHOP_IDS_KEY;

/** 预热展示数据；秒杀库存、购买资格与pending不属于缓存预热范围。 */
@Service
@RequiredArgsConstructor
public class CacheWarmupService {
    private final ShopTypeCacheService shopTypes;
    private final VoucherListCacheService vouchers;
    private final ShopMapper shops;
    private final StringRedisTemplate redis;
    private final CacheWarmupProperties properties;
    private final VoucherMapper voucherMapper;

    /** 只在Redis预热阶段查业务库；本地广播和启动预热均读取已发布名单。 */
    public List<Long> findShopIdsToWarm() {
        if (!properties.getHotShopIds().isEmpty()) {
            return List.copyOf(new LinkedHashSet<>(properties.getHotShopIds()));
        }
        LocalDateTime now = LocalDateTime.now();
        return voucherMapper.findWarmupShopIds(now, now.plus(properties.getAheadOfStart()), properties.getMaxShops());
    }

    /** 单次SET原子替换名单，包括空列表；不先删除旧名单，失败时保留上一次成功结果。 */
    public void publishShopIds(List<Long> shopIds) {
        redis.opsForValue().set(CACHE_WARMUP_SHOP_IDS_KEY, JSONUtil.toJsonStr(shopIds),
                Expiration.from(properties.getShopListTtl()));
    }

    public List<Long> loadPublishedShopIds() {
        String json = redis.opsForValue().get(CACHE_WARMUP_SHOP_IDS_KEY);
        if (StrUtil.isBlank(json)) {
            throw new IllegalStateException("预热商铺名单缺失，请先完成Redis预热");
        }
        // []表示本轮没有活动商铺，与尚未发布名单不同。
        return JSONUtil.toList(json, Long.class);
    }

    public void warmShopTypesRedis() { shopTypes.warmRedis(properties.getMinRemainingTtl()); }
    public void warmVouchersRedis(Long shopId) { vouchers.warmRedis(shopId, properties.getMinRemainingTtl()); }

    public void warmShopRedis(Long shopId) {
        String key = CACHE_SHOP_KEY + shopId;
        String json = redis.opsForValue().get(key);
        if (json != null) {
            // 尊重现有短期空值缓存，不因预热重复查询不存在的商铺。
            if (StrUtil.isBlank(json) || "null".equals(json)) return;
            RedisData existing = JSONUtil.toBean(json, RedisData.class);
            if (existing.getExpireTime() != null) {
                if (existing.getExpireTime().isAfter(LocalDateTime.now().plus(properties.getMinRemainingTtl()))) return;
            } else {
                // 兼容已有普通商铺JSON；刷新时统一写入查询端支持的逻辑过期格式。
                Long ttl = redis.getExpire(key, TimeUnit.MILLISECONDS);
                if (ttl != null && ttl > properties.getMinRemainingTtl().toMillis()) return;
            }
        }
        Shop shop = shops.selectById(shopId);
        if (shop == null) {
            redis.opsForValue().set(key, "", Expiration.from(CACHE_NULL_TTL, TimeUnit.MINUTES));
            return;
        }
        RedisData value = new RedisData();
        value.setData(shop);
        value.setExpireTime(LocalDateTime.now().plus(properties.getShopTtl()));
        redis.opsForValue().set(key, JSONUtil.toJsonStr(value));
    }

    public void warmShopTypesLocal() {
        if (!shopTypes.warmLocalFromRedis()) throw new IllegalStateException("分类Redis缓存缺失，请先执行Redis预热");
    }

    public void warmVouchersLocal(Long shopId) {
        if (!vouchers.warmLocalFromRedis(shopId)) {
            throw new IllegalStateException("优惠券Redis缓存缺失，请先执行Redis预热，shopId=" + shopId);
        }
    }
}
