package com.hmdp.cache.config;

import com.hmdp.framework.cache.config.LocalCacheProperties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "hmdp.cache.voucher-list")
public class VoucherCacheProperties extends LocalCacheProperties {
}
