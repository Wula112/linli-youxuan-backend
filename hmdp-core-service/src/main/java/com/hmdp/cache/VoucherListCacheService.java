package com.hmdp.cache;

import com.hmdp.cache.config.VoucherCacheProperties;
import com.hmdp.entity.Voucher;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.framework.cache.LocalCacheClient;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.time.Duration;

import static com.hmdp.common.constant.RedisConstants.CACHE_SHOP_VOUCHER_KEY;

@Slf4j
@Service
public class VoucherListCacheService {

    @Resource
    private VoucherMapper voucherMapper;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private VoucherCacheProperties cacheProperties;

    // 按商铺id缓存优惠券列表，首次访问时才加载数据
    private LocalCacheClient<Long, Voucher> voucherListCache;

    @PostConstruct
    public void initCache() {
        voucherListCache = new LocalCacheClient<>(stringRedisTemplate, cacheProperties, Voucher.class,
                shopId -> CACHE_SHOP_VOUCHER_KEY + shopId, voucherMapper::queryVoucherOfShop);
        log.info("优惠券列表缓存初始化完成，mode={}", cacheProperties.getMode());
    }

    public List<Voucher> getVoucherByShopId(Long shopId) {
        return voucherListCache.get(shopId);
    }

    public void invalidateLocalCache(Long shopId) {
        voucherListCache.invalidate(shopId);
    }

    public void warmRedis(Long shopId, Duration minRemainingTtl) {
        voucherListCache.warmRedis(shopId, minRemainingTtl);
    }

    public boolean warmLocalFromRedis(Long shopId) {
        return voucherListCache.warmLocalFromRedis(shopId);
    }
}
