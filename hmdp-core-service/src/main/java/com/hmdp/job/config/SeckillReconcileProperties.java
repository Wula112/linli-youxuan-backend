package com.hmdp.job.config;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.time.Duration;

@Data
@Component
@ConfigurationProperties(prefix = "hmdp.seckill.reconcile")
public class SeckillReconcileProperties {

    private boolean enabled = true;
    private Duration interval = Duration.ofMinutes(1);
    // 初次核查的超时阈值，也是每次核查之间的最短等待时间。
    private Duration pendingTimeout = Duration.ofMinutes(5);
    private int batchSize = 100;

    @PostConstruct
    public void validate() {
        Assert.isTrue(interval != null && interval.toMillis() > 0, "interval必须大于0毫秒");
        Assert.isTrue(pendingTimeout != null && pendingTimeout.toMillis() > 0, "pending-timeout必须大于0毫秒");
        Assert.isTrue(batchSize > 0 && batchSize <= 1000, "batch-size必须在1到1000之间");
    }
}
