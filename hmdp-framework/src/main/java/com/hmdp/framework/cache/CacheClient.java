package com.hmdp.framework.cache;

import com.hmdp.common.constant.RedisConstants;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.common.constant.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.common.constant.RedisConstants.LOCK_SHOP_KEY;

@Component
@Slf4j
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;
    private static final ExecutorService CACHE_REBUILT_EXECUTOR = Executors.newFixedThreadPool(10);


    /**
     * Lua：
     * 判断锁是不是自己的，如果是才删除。
     */
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;

    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setScriptText(
                "if redis.call('get', KEYS[1]) == ARGV[1] then " +
                        "return redis.call('del', KEYS[1]) " +
                        "else return 0 end"
        );
        UNLOCK_SCRIPT.setResultType(Long.class);
    }



    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), Expiration.from(time,unit));
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        RedisData redisData = new RedisData();
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        redisData.setData(value);
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    // 解决缓存穿透
    public <ID,R> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1. 从redis查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2. 缓存命中，直接返回
        if(StrUtil.isNotBlank(json)){
            return JSONUtil.toBean(json, type);
        }
        if(json != null){
            return null;
        }
        // 3. 缓存未命中，查询数据库
        R r = dbFallback.apply(id);
        // 4. 数据库也没有，设置空缓存对象，防缓存穿透
        if(r == null){
            stringRedisTemplate.opsForValue().set(key,"", Expiration.from(RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES));
            return null;
        }
        // 5. 查到数据，写入Redis缓存
        set(key,r,time,unit);
        // 6. 返回数据
        return r;
    }

    // 逻辑过期 解决缓存击穿
    public <ID,R> R queryLogicalExpire(String keyPrefix, String lockKeyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix + id;
        // 1. 从redis查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2. 缓存未命中，直接返回
        if(StrUtil.isBlank(json)){
            return null;
        }
        // 3. 缓存命中，查看缓存是否过期
        RedisData bean = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) bean.getData(), type);
        // 4. 没过期，直接返回
        if(bean.getExpireTime().isAfter(LocalDateTime.now())){
            return r;
        }
        // 5. 过期，缓存重建
        // 5.1 尝试获取锁
        String lockKey = lockKeyPrefix + id;
        String lockValue = UUID.randomUUID().toString();
        if(tryLock(lockKey,lockValue)){
            // 5.2 二次校验在该线程获取锁时，缓存是否被其他线程已经重建
            String latestJson = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(latestJson)) {
                RedisData latestRedisData = JSONUtil.toBean(latestJson, RedisData.class);
                R latestData = JSONUtil.toBean((JSONObject) latestRedisData.getData(), type);
                if (latestRedisData.getExpireTime().isAfter(LocalDateTime.now())) {
                    // 已经被其他线程重建
                    unLock(lockKey,lockValue);
                    return latestData;
                }
            }
            try {
                // 5.3 交给其他线程重建缓存
                CACHE_REBUILT_EXECUTOR.submit(() -> {
                    try {
                        // 1. 查数据库(调用方提供查询方法)
                        R newData = dbFallback.apply(id);
                        // 2. 写入Redis
                        this.setWithLogicalExpire(key, newData, time, unit);
                    } catch (Exception e) {
                        log.error("缓存重建失败，key={}", key, e);
                        throw new RuntimeException(e);
                    } finally {
                        unLock(lockKey,lockValue);
                    }
                });
            } catch (RejectedExecutionException e) {
                /*
                 * 如果线程池拒绝任务，
                 * 当前线程已经拿到了锁，
                 * 所以一定要释放。
                 */
                unLock(lockKey,lockValue);
                log.error("缓存重建失败，key={}", key, e);
                throw new RuntimeException(e);
            }

        }
        // 6 当前线程先返回旧数据
        return r;
    }

    private boolean tryLock(String key,String value){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, value, Expiration.seconds(10));
        return BooleanUtil.isTrue(flag);
    }

    // =========================
    // 安全释放 Redis 锁
    // =========================

    private void unLock(String key, String value) {
        stringRedisTemplate.execute(UNLOCK_SCRIPT, Collections.singletonList(key), value);
    }

}
