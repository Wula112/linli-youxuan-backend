package com.hmdp.framework.cache.config;

import lombok.Data;

import java.time.Duration;

/**
 * 列表多级缓存的通用配置，由具体业务的配置类绑定不同前缀。
 */
@Data
public class LocalCacheProperties {

    /**
     * mysql：仅DB；redis：Redis + DB；caffeine：本地缓存 + Redis + DB
     */
    private String mode = "caffeine";

    /**
     * 本地缓存最多保留的key数量
     */
    private long maximumSize = 1000;

    /**
     * 写入后多久允许异步刷新，下次访问时触发
     */
    private Duration refreshAfterWrite = Duration.ofSeconds(5);

    /**
     * 本地缓存写入后的硬过期时间
     */
    private Duration expireAfterWrite = Duration.ofMinutes(10);

    /**
     * Redis中非空列表的过期时间，空列表沿用CACHE_NULL_TTL
     */
    private Duration redisTtl = Duration.ofMinutes(10);
}
