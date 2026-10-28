package com.hmdp.framework.cache;

import com.hmdp.common.constant.RedisConstants;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.hmdp.framework.cache.config.LocalCacheProperties;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.common.constant.RedisConstants.CACHE_NULL_TTL;

/**
 * 列表数据的多级缓存工具，每个业务持有独立实例。
 * K为本地缓存key类型，T为列表元素类型；Redis key和数据库查询由调用方提供。
 */
public class LocalCacheClient<K, T> {

    private final StringRedisTemplate stringRedisTemplate;
    private final LocalCacheProperties cacheProperties;
    private final Class<T> elementType;
    private final Function<K, String> redisKeyBuilder;
    private final Function<K, List<T>> dbFallback;
    private final LoadingCache<K, List<T>> localCache;

    public LocalCacheClient(StringRedisTemplate stringRedisTemplate, LocalCacheProperties cacheProperties,
                            Class<T> elementType, Function<K, String> redisKeyBuilder,
                            Function<K, List<T>> dbFallback) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.cacheProperties = cacheProperties;
        this.elementType = elementType;
        this.redisKeyBuilder = redisKeyBuilder;
        this.dbFallback = dbFallback;

        Assert.isTrue(List.of("mysql", "redis", "caffeine").contains(getCacheMode()),
                "缓存mode只能配置为mysql、redis或caffeine");
        Assert.isTrue(cacheProperties.getMaximumSize() > 0, "maximum-size必须大于0");
        long refreshMillis = positiveMillis(cacheProperties.getRefreshAfterWrite(), "refresh-after-write");
        long expireMillis = positiveMillis(cacheProperties.getExpireAfterWrite(), "expire-after-write");
        positiveMillis(cacheProperties.getRedisTtl(), "redis-ttl");

        // 只创建容器和加载规则，首次get时才访问Redis或数据库
        localCache = Caffeine.newBuilder()
                .maximumSize(cacheProperties.getMaximumSize())
                .refreshAfterWrite(refreshMillis, TimeUnit.MILLISECONDS)
                .expireAfterWrite(expireMillis, TimeUnit.MILLISECONDS)
                .build(this::loadFromRedisThenDb);
    }

    public List<T> get(K key) {
        switch (getCacheMode()) {
            case "mysql":
                // 每次直接查库，不读取或回填两级缓存
                return loadFromDb(key);
            case "redis":
                // 跳过本地缓存，未命中时回源数据库并回填Redis
                return loadFromRedisThenDb(key);
            case "caffeine":
                // 同一实例内，相同key的并发首次加载由LoadingCache合并
                return localCache.get(key);
            default:
                throw new IllegalArgumentException("不支持的缓存模式：" + cacheProperties.getMode());
        }
    }

    private List<T> loadFromRedisThenDb(K key) {
        String redisKey = redisKeyBuilder.apply(key);
        String json = stringRedisTemplate.opsForValue().get(redisKey);
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toList(json, elementType);
        }

        List<T> list = loadFromDb(key);
        stringRedisTemplate.opsForValue().set(redisKey, JSONUtil.toJsonStr(list),
                Expiration.from(ttlMillis(list), TimeUnit.MILLISECONDS));
        return list;
    }

    private List<T> loadFromDb(K key) {
        List<T> list = dbFallback.apply(key);
        return list == null ? Collections.emptyList() : list;
    }

    public void invalidate(K key) {
        localCache.invalidate(key);
    }

    public void invalidateAll() {
        localCache.invalidateAll();
    }

    /** Redis预热由单个实例执行；余量充足时不回源，不删除现有缓存。 */
    public void warmRedis(K key, Duration minRemainingTtl) {
        if ("mysql".equals(getCacheMode())) return;
        String redisKey = redisKeyBuilder.apply(key);
        Long ttl = stringRedisTemplate.getExpire(redisKey, TimeUnit.MILLISECONDS);
        if (ttl != null && ttl > minRemainingTtl.toMillis()) return;
        List<T> list = loadFromDb(key);
        stringRedisTemplate.opsForValue().set(redisKey, JSONUtil.toJsonStr(list),
                Expiration.from(ttlMillis(list), TimeUnit.MILLISECONDS));
    }

    /** 每个实例从Redis装载自己的L1，绕过已有L1值；缺失时不让广播任务集体回源。 */
    public boolean warmLocalFromRedis(K key) {
        if (!"caffeine".equals(getCacheMode())) return true;
        String json = stringRedisTemplate.opsForValue().get(redisKeyBuilder.apply(key));
        if (StrUtil.isBlank(json)) return false;
        localCache.put(key, JSONUtil.toList(json, elementType));
        return true;
    }

    private long ttlMillis(List<T> list) {
        return list.isEmpty() ? TimeUnit.MINUTES.toMillis(CACHE_NULL_TTL) : cacheProperties.getRedisTtl().toMillis();
    }

    private String getCacheMode() {
        String mode = cacheProperties.getMode();
        Assert.hasText(mode, "缓存mode不能为空");
        return mode.trim().toLowerCase(Locale.ROOT);
    }

    private long positiveMillis(Duration duration, String property) {
        Assert.isTrue(duration != null && duration.toMillis() > 0, property + "必须大于0毫秒");
        return duration.toMillis();
    }
}
