package com.hmdp.service.impl;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import static com.hmdp.common.constant.RedisConstants.SECKILL_CIRCUIT_KEY;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SeckillCircuitBreakerTest {
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final ValueOperations<String, String> values = mock(ValueOperations.class);
    private final SeckillCircuitBreaker circuit = new SeckillCircuitBreaker(redis);

    @Test
    void retainsFirstIncidentWithoutAnAutomaticExpiry() {
        when(redis.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(SECKILL_CIRCUIT_KEY, "msg-1")).thenReturn(true);
        when(values.setIfAbsent(SECKILL_CIRCUIT_KEY, "msg-2")).thenReturn(false);
        circuit.trip("msg-1");
        circuit.trip("msg-2");
        verify(values).setIfAbsent(SECKILL_CIRCUIT_KEY, "msg-1");
        verify(values).setIfAbsent(SECKILL_CIRCUIT_KEY, "msg-2");
    }

    @Test
    void cannotTreatAnUnknownCircuitStateAsClosed() {
        when(redis.hasKey(SECKILL_CIRCUIT_KEY)).thenReturn(null);
        assertThrows(IllegalStateException.class, circuit::isOpen);
    }
}
