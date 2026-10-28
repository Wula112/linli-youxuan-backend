package com.hmdp.cache;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.cache.config.ShopTypeCacheProperties;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.framework.cache.LocalCacheClient;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.time.Duration;

import static com.hmdp.common.constant.RedisConstants.CACHE_SHOP_TYPE_KEY;

@Slf4j
@Service
public class ShopTypeCacheService {

    @Resource
    private ShopTypeMapper shopTypeMapper;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private ShopTypeCacheProperties cacheProperties;

    private LocalCacheClient<String, ShopType> shopTypeCache;

    @PostConstruct
    public void initCache() {
        shopTypeCache = new LocalCacheClient<>(stringRedisTemplate, cacheProperties, ShopType.class,
                key -> key, key -> shopTypeMapper.selectList(new QueryWrapper<ShopType>().orderByAsc("sort")));
        log.info("商铺分类缓存初始化完成，mode={}", cacheProperties.getMode());
    }

    public List<ShopType> getTypeList() {
        // 整个分类列表使用一个固定key，保持原有Redis key不变
        return shopTypeCache.get(CACHE_SHOP_TYPE_KEY);
    }

    public void invalidateLocalCache() {
        shopTypeCache.invalidate(CACHE_SHOP_TYPE_KEY);
    }

    public void warmRedis(Duration minRemainingTtl) {
        shopTypeCache.warmRedis(CACHE_SHOP_TYPE_KEY, minRemainingTtl);
    }

    public boolean warmLocalFromRedis() {
        return shopTypeCache.warmLocalFromRedis(CACHE_SHOP_TYPE_KEY);
    }
}
