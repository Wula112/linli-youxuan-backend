package com.hmdp.cache.config;

import com.hmdp.framework.cache.config.LocalCacheProperties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@ConfigurationProperties(prefix = "hmdp.cache.shop-type")
public class ShopTypeCacheProperties extends LocalCacheProperties {

    public ShopTypeCacheProperties() {
        // 分类只有一个列表，保留原来的Redis过期时间
        setMaximumSize(1);
        setRedisTtl(Duration.ofMinutes(30));
    }
}
