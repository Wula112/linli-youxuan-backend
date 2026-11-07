package com.hmdp.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import static com.hmdp.common.constant.RedisConstants.SECKILL_CIRCUIT_KEY;

/** 下单死信触发全局熔断；保留首个事件直到故障核查后手动恢复。 */
@Service
@RequiredArgsConstructor
public class SeckillCircuitBreaker {
    private final StringRedisTemplate redis;

    public void trip(String messageId) {
        String incident = messageId == null || messageId.isBlank() ? "unknown" : messageId;
        Boolean stored = redis.opsForValue().setIfAbsent(SECKILL_CIRCUIT_KEY, incident);
        if (stored == null) throw new IllegalStateException("无法确认秒杀熔断状态");
    }

    public boolean isOpen() {
        Boolean present = redis.hasKey(SECKILL_CIRCUIT_KEY);
        if (present == null) throw new IllegalStateException("无法读取秒杀熔断状态");
        return present;
    }

    public String incident() {
        return redis.opsForValue().get(SECKILL_CIRCUIT_KEY);
    }
}
