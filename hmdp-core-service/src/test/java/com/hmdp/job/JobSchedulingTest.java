package com.hmdp.job;

import com.hmdp.framework.job.config.XxlJobConfig;
import com.hmdp.framework.job.config.XxlJobProperties;
import com.hmdp.job.config.JobProperties;
import com.hmdp.job.config.OrderTimeoutConfig;
import com.hmdp.job.config.SeckillReconcileConfig;
import com.xxl.job.core.executor.impl.XxlJobSpringExecutor;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class JobSchedulingTest {
    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties
    static class Binding { }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withUserConfiguration(Binding.class, JobProperties.class, OrderTimeoutConfig.class, SeckillReconcileConfig.class)
                .withBean(SeckillOrderReconcileJob.class, () -> mock(SeckillOrderReconcileJob.class))
                .withBean(OrderTimeoutJob.class, () -> mock(OrderTimeoutJob.class));
    }

    @Test
    void shouldUseOnlyLocalSchedulersByDefault() {
        runner().withUserConfiguration(XxlJobConfig.class).run(context -> {
            assertNull(context.getStartupFailure());
            assertTrue(context.containsBean("seckillReconcileScheduler"));
            assertTrue(context.containsBean("orderTimeoutScheduler"));
            assertTrue(context.getBeansOfType(XxlJobSpringExecutor.class).isEmpty());
        });
    }

    @Test
    void shouldDisableBothLocalSchedulersInXxlMode() {
        runner().withPropertyValues("hmdp.job.scheduler=xxl").run(context -> {
            assertNull(context.getStartupFailure());
            assertFalse(context.containsBean("seckillReconcileScheduler"));
            assertFalse(context.containsBean("orderTimeoutScheduler"));
        });
    }

    @Test
    void shouldKeepFeatureSwitchesAndRejectInvalidMode() {
        runner().withPropertyValues("hmdp.seckill.reconcile.enabled=false", "hmdp.order.timeout-scan-enabled=false")
                .run(context -> {
                    assertFalse(context.containsBean("seckillReconcileScheduler"));
                    assertFalse(context.containsBean("orderTimeoutScheduler"));
                });
        runner().withPropertyValues("hmdp.job.scheduler=typo").run(context -> assertNotNull(context.getStartupFailure()));
    }

    @Test
    void shouldValidateExecutorConfigurationWithoutStartingAConnection() {
        var config = new XxlJobConfig();
        var properties = new XxlJobProperties();
        assertThrows(IllegalArgumentException.class, () -> config.xxlJobExecutor(properties));
        properties.setAdminAddresses("http://127.0.0.1:8080");
        properties.setAccessToken("test-only");
        properties.setPort(19999);
        var executor = config.xxlJobExecutor(properties);
        assertEquals(19999, executor.getPort());
        assertEquals("hmdp-job-executor", executor.getAppname());
        assertEquals("test-only", executor.getAccessToken());
    }
}
