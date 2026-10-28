package com.hmdp.job;

import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.mq.producer.VoucherOrderPublisher;
import com.hmdp.job.config.SeckillReconcileConfig;
import com.hmdp.job.config.JobProperties;
import com.hmdp.job.config.SeckillReconcileProperties;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SeckillReconcileConfigTest {

    @Test
    void shouldDisableJobAndDedicatedSchedulerTogether() {
        new ApplicationContextRunner()
                .withUserConfiguration(BindingConfiguration.class, SeckillReconcileConfig.class,
                        SeckillReconcileProperties.class, SeckillOrderReconcileJob.class)
                .withPropertyValues("hmdp.seckill.reconcile.enabled=false")
                .run(ctx -> {
                    assertNull(ctx.getStartupFailure());
                    assertFalse(ctx.containsBean("seckillReconcileScheduler"));
                    assertTrue(ctx.getBeansOfType(SeckillOrderReconcileJob.class).isEmpty());
                });
    }

    @Test
    void shouldAcceptMinuteIntervalAndUseDedicatedScheduler() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.opsForZSet()).thenReturn(mock(ZSetOperations.class));
        new ApplicationContextRunner()
                .withUserConfiguration(BindingConfiguration.class, SeckillReconcileConfig.class,
                        SeckillReconcileProperties.class, SeckillOrderReconcileJob.class)
                .withBean(JobProperties.class, JobProperties::new)
                .withBean(StringRedisTemplate.class, () -> redis)
                .withBean(VoucherOrderMapper.class, () -> mock(VoucherOrderMapper.class))
                .withBean(VoucherOrderPublisher.class, () -> mock(VoucherOrderPublisher.class))
                .withBean(RedissonClient.class, () -> mock(RedissonClient.class))
                .withPropertyValues("hmdp.seckill.reconcile.interval=1m", "hmdp.seckill.reconcile.max-republish-retries=2")
                .run(ctx -> {
                    assertNull(ctx.getStartupFailure());
                    assertTrue(ctx.containsBean("seckillReconcileScheduler"));
                    assertEquals(1, ctx.getBeansOfType(SeckillOrderReconcileJob.class).size());
                    assertEquals(2, ctx.getBean(SeckillReconcileProperties.class).getMaxRepublishRetries());
                });
    }

    @Test
    void shouldValidateBoundsAndKeepConservativeDefaults() {
        var properties = new SeckillReconcileProperties();
        assertEquals(Duration.ofMinutes(1), properties.getInterval());
        assertEquals(Duration.ofMinutes(5), properties.getPendingTimeout());
        assertEquals(100, properties.getBatchSize());
        assertEquals(5, properties.getMaxRepublishRetries());
        assertDoesNotThrow(properties::validate);
        properties.setBatchSize(0);
        assertThrows(IllegalArgumentException.class, properties::validate);
    }

    @Test
    void shouldRejectNegativeRetryLimitAndAcceptZero() {
        var properties = new SeckillReconcileProperties();
        properties.setMaxRepublishRetries(0);
        assertDoesNotThrow(properties::validate);
        properties.setMaxRepublishRetries(-1);
        assertThrows(IllegalArgumentException.class, properties::validate);
    }

    @Test
    void shouldRecoverOriginalMessageFromCompletePendingMember() {
        VoucherOrderMessage message = new VoucherOrderMessage(1000L, 5L, 10L);
        assertEquals("10:5:1000", message.pendingMember());
        assertEquals(message, VoucherOrderMessage.fromPendingMember(message.pendingMember()));
        assertThrows(IllegalArgumentException.class, () -> VoucherOrderMessage.fromPendingMember("10:1000"));
        assertThrows(IllegalArgumentException.class, () -> VoucherOrderMessage.fromPendingMember("10::1000"));
        assertThrows(IllegalArgumentException.class, () -> VoucherOrderMessage.fromPendingMember("10:5:-1"));
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties
    static class BindingConfiguration {
    }
}
