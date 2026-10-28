package com.hmdp.job.config;

import com.hmdp.job.OrderTimeoutJob;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
@EnableScheduling
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "hmdp.order", name = "timeout-scan-enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnProperty(prefix = "hmdp.job", name = "scheduler", havingValue = "local", matchIfMissing = true)
public class OrderTimeoutConfig {

    private final OrderTimeoutJob job;

    @Scheduled(fixedDelayString = "${hmdp.order.timeout-scan-interval:60000}", scheduler = "orderTimeoutScheduler")
    public void closeLocally() { job.closeExpiredOrders(); }

    @Bean
    public ThreadPoolTaskScheduler orderTimeoutScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("order-timeout-");
        return scheduler;
    }
}
