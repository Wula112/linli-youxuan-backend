package com.hmdp.cache.config;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Data
@Component
@ConfigurationProperties(prefix = "hmdp.cache.warmup")
public class CacheWarmupProperties {
    private boolean enabled = false;
    private boolean startupLocal = true;
    // 原规则（保留）：所有实例使用相同名单；空名单只预热分类，不全表扫描商铺。
    // 现在默认为空，按活动自动筛选；非空仅用于压测/临时指定，覆盖自动筛选。
    private List<Long> hotShopIds = new ArrayList<>();
    // 自动选择正在进行、以及未来10分钟内开始的秒杀活动所属商铺。
    private Duration aheadOfStart = Duration.ofMinutes(10);
    private int maxShops = 100;
    // 最新成功名单的有效期；过期后本地任务报缺失，不广播回源数据库。
    private Duration shopListTtl = Duration.ofMinutes(5);
    private Duration minRemainingTtl = Duration.ofMinutes(2);
    private Duration shopTtl = Duration.ofMinutes(30);
    // 首次 + 最多2次重试，默认等待1s、2s；Admin失败重试设为0。
    private int maxRetries = 2;
    private Duration retryDelay = Duration.ofSeconds(1);
    private Duration maxRetryDelay = Duration.ofSeconds(5);

    @PostConstruct
    public void validate() {
        Assert.isTrue(hotShopIds != null && hotShopIds.size() <= 1000
                && hotShopIds.stream().allMatch(id -> id != null && id > 0),
                "hot-shop-ids最多1000个，且必须为正数");
        Assert.isTrue(aheadOfStart != null && !aheadOfStart.isNegative(), "ahead-of-start不能为负数");
        Assert.isTrue(maxShops > 0 && maxShops <= 1000, "max-shops必须在1到1000之间");
        Assert.isTrue(shopListTtl != null && shopListTtl.toMillis() > 0, "shop-list-ttl必须大于0");
        Assert.isTrue(minRemainingTtl != null && minRemainingTtl.toMillis() > 0,
                "min-remaining-ttl必须大于0");
        Assert.isTrue(shopTtl != null && shopTtl.compareTo(minRemainingTtl) > 0,
                "shop-ttl必须大于min-remaining-ttl");
        Assert.isTrue(maxRetries >= 0 && maxRetries <= 3, "预热max-retries必须在0到3之间");
        Assert.isTrue(retryDelay != null && retryDelay.toMillis() > 0,
                "retry-delay必须大于0");
        Assert.isTrue(maxRetryDelay != null && maxRetryDelay.compareTo(retryDelay) >= 0
                && maxRetryDelay.compareTo(Duration.ofSeconds(5)) <= 0,
                "max-retry-delay必须不小于retry-delay，且不超过5秒");
    }
}
