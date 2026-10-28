package com.hmdp.job.config;

import com.hmdp.job.SeckillOrderReconcileJob;
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
@ConditionalOnProperty(prefix = "hmdp.seckill.reconcile", name = "enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnProperty(prefix = "hmdp.job", name = "scheduler", havingValue = "local", matchIfMissing = true)
public class SeckillReconcileConfig {

    private final SeckillOrderReconcileJob job;

    @Scheduled(fixedDelayString = "${hmdp.seckill.reconcile.interval:60000}", scheduler = "seckillReconcileScheduler")
    public void reconcileLocally() { job.reconcile(); }

    @Bean
    public ThreadPoolTaskScheduler seckillReconcileScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("seckill-reconcile-");
        return scheduler;
    }
}
