package com.hmdp.cache.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Configuration
public class CacheConfig {

    @Bean(destroyMethod = "shutdown")
    public ScheduledExecutorService cacheDeleteExecutor() {
        return Executors.newSingleThreadScheduledExecutor(runnable ->
                new Thread(runnable, "cache-delete"));
    }
}
