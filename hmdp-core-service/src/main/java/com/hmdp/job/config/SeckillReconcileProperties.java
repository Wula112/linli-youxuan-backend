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
    // 初次核查的超时阈值，也是每次核查/补发之间的最短等待时间。
    private Duration pendingTimeout = Duration.ofMinutes(5);
    private int batchSize = 100;
    // 每笔订单允许的对账补发轮次；单轮仍使用Publisher既有重试。0表示只核查、不补发。
    // 上限大于0时，耗尽后的下一次超时核查会持订单锁重新查库，确认未建单才释放预占。
    private int maxRepublishRetries = 5;

    @PostConstruct
    public void validate() {
        Assert.isTrue(interval != null && interval.toMillis() > 0, "interval必须大于0毫秒");
        Assert.isTrue(pendingTimeout != null && pendingTimeout.toMillis() > 0, "pending-timeout必须大于0毫秒");
        Assert.isTrue(batchSize > 0 && batchSize <= 1000, "batch-size必须在1到1000之间");
        Assert.isTrue(maxRepublishRetries >= 0, "max-republish-retries不能为负");
    }
}
