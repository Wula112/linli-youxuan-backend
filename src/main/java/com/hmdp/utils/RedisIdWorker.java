package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


@Component
public class RedisIdWorker {

    private static final long BEGIN_TIMESTAMP = 1672531200L;

    private static final int COUNT_BITS = 32;

    private StringRedisTemplate stringRedisTemplate;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public long nextId(String keyPrefix){
        // 1. 生成时间戳（当前时间戳 - 某个时间节点的时间戳）
        long nowSecond = Instant.now().getEpochSecond();
        long timeStamp = nowSecond - BEGIN_TIMESTAMP;

        // 2. 生成序列号（Redis 的String类型 value自增）
        String date = LocalDateTime.now().format(DateTimeFormatter.BASIC_ISO_DATE);
        Long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);

        // 3. 拼接时间戳 和 序列号
        return timeStamp << COUNT_BITS | count;
    }


}
