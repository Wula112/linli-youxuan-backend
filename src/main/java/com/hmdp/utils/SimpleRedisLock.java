package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.core.types.Expiration;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

// 分布式锁 - Redis
public class SimpleRedisLock implements ILock{


    private StringRedisTemplate stringRedisTemplate;
    private String name;
    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";

    public SimpleRedisLock(StringRedisTemplate stringRedisTemplate, String name) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.name = name;
    }

    private static final DefaultRedisScript<Long> REDIS_SCRIPT;

    static {
        REDIS_SCRIPT = new DefaultRedisScript<>();
        REDIS_SCRIPT.setLocation(new ClassPathResource("UnLock.lua"));
        REDIS_SCRIPT.setResultType(Long.class);
    }

    @Override
    public boolean tryLock(Long timeSeconds) {
        // 1. 获取锁
        String value = ID_PREFIX + Thread.currentThread().threadId();
        Boolean success = stringRedisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + name, value, Expiration.seconds(timeSeconds));
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unlock() {
        stringRedisTemplate.execute(REDIS_SCRIPT,
                Collections.singletonList(KEY_PREFIX + name),
                ID_PREFIX + Thread.currentThread().threadId());

    }
}
