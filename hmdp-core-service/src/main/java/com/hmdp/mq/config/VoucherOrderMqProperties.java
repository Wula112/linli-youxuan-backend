package com.hmdp.mq.config;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.time.Duration;

@Data
@Component
@ConfigurationProperties(prefix = "hmdp.mq.voucher-order")
public class VoucherOrderMqProperties {

    private int producerMaxAttempts = 3;
    private Duration retryInterval = Duration.ofMillis(200);
    private Duration confirmTimeout = Duration.ofSeconds(3);

    @PostConstruct
    public void validate() {
        Assert.isTrue(producerMaxAttempts > 0, "producer-max-attempts必须大于0");
        Assert.isTrue(retryInterval != null && retryInterval.toMillis() > 0, "retry-interval必须大于0毫秒");
        Assert.isTrue(confirmTimeout != null && confirmTimeout.toMillis() > 0, "confirm-timeout必须大于0毫秒");
    }
}
