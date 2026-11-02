package com.hmdp.cache;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.cache.config.VoucherCacheProperties;
import com.hmdp.entity.Voucher;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.framework.cache.LocalCacheClient;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.time.Duration;

import static com.hmdp.common.constant.RedisConstants.CACHE_SHOP_VOUCHER_KEY;
import static com.hmdp.common.constant.RedisConstants.SECKILL_STOCK_KEY;

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

    /**
     * 券列表缓存只保存元数据，库存是随下单持续变化的计数器，不参与多级缓存。
     *
     * <p>库存读取 {@code seckill:stock:{voucherId}}（与秒杀预扣、回退同一个 Key），
     * 因此下单和关单都不需要广播失效消息。缓存里的 stock 仅作为 Key 缺失时的兜底值。
     * mysql 模式仍只查库，不额外访问 Redis。
     */
    public List<Voucher> getVoucherByShopId(Long shopId) {
        List<Voucher> cached = voucherListCache.get(shopId);
        if ("mysql".equalsIgnoreCase(cacheProperties.getMode().trim())) {
            return cached;
        }
        return withRedisStock(cached);
    }

    /** 复制缓存对象后再覆盖库存，避免把 Redis 值写回 Caffeine 里的共享实例。 */
    private List<Voucher> withRedisStock(List<Voucher> cached) {
        if (cached.isEmpty()) {
            return cached;
        }
        List<Voucher> vouchers = new ArrayList<>(cached.size());
        for (Voucher voucher : cached) {
            vouchers.add(BeanUtil.copyProperties(voucher, Voucher.class));
        }
        List<String> stockKeys = vouchers.stream()
                .filter(voucher -> voucher.getStock() != null)
                .map(voucher -> SECKILL_STOCK_KEY + voucher.getId())
                .toList();
        if (stockKeys.isEmpty()) {
            return vouchers;
        }
        List<String> stocks;
        try {
            stocks = stringRedisTemplate.opsForValue().multiGet(stockKeys);
        } catch (Exception ex) {
            log.error("读取秒杀库存失败，回退数据库库存，keys={}", stockKeys, ex);
            return vouchers;
        }
        if (stocks == null || stocks.size() != stockKeys.size()) {
            return vouchers;
        }
        int stockIndex = 0;
        for (Voucher voucher : vouchers) {
            if (voucher.getStock() == null) {
                continue;
            }
            Integer stock = parseStock(stocks.get(stockIndex++));
            if (stock != null) {
                voucher.setStock(stock);
            }
        }
        return vouchers;
    }

    private Integer parseStock(String value) {
        if (value == null) {
            return null;
        }
        try {
            return Integer.valueOf(value);
        } catch (NumberFormatException ex) {
            log.error("秒杀库存值非法，回退数据库库存，value={}", value, ex);
            return null;
        }
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
