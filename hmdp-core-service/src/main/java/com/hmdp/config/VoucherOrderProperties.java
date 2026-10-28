package com.hmdp.config;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.time.Duration;

@Data
@Component
@ConfigurationProperties(prefix = "hmdp.order")
public class VoucherOrderProperties {

    // 从数据库建单时开始计算，预占和消息排队时间不占用支付窗口。
    private Duration payTimeout = Duration.ofMinutes(15);
    private boolean timeoutScanEnabled = true;
    private Duration timeoutScanInterval = Duration.ofMinutes(1);
    private int timeoutScanBatchSize = 100;

    @PostConstruct
    public void validate() {
        Assert.isTrue(payTimeout != null && payTimeout.toMillis() > 0, "pay-timeout必须大于0毫秒");
        Assert.isTrue(timeoutScanInterval != null && timeoutScanInterval.toMillis() > 0,
                "timeout-scan-interval必须大于0毫秒");
        Assert.isTrue(timeoutScanBatchSize > 0 && timeoutScanBatchSize <= 1000,
                "timeout-scan-batch-size必须在1到1000之间");
    }
}
