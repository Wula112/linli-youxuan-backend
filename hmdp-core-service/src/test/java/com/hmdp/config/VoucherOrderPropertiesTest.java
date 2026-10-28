package com.hmdp.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class VoucherOrderPropertiesTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(BindingConfiguration.class);

    @Test
    void shouldUseFifteenMinuteDefaultAndBindConfiguredTimeout() {
        runner.run(ctx -> assertEquals(Duration.ofMinutes(15),
                ctx.getBean(VoucherOrderProperties.class).getPayTimeout()));
        runner.withPropertyValues("hmdp.order.pay-timeout=30s").run(ctx -> {
            assertNull(ctx.getStartupFailure());
            assertEquals(Duration.ofSeconds(30), ctx.getBean(VoucherOrderProperties.class).getPayTimeout());
        });
    }

    @Test
    void shouldFailStartupForNonPositiveTimeout() {
        runner.withPropertyValues("hmdp.order.pay-timeout=0s")
                .run(ctx -> assertNotNull(ctx.getStartupFailure()));
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(VoucherOrderProperties.class)
    static class BindingConfiguration {
    }
}
