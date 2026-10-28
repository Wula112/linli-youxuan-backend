package com.hmdp.mq.config;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.time.Duration;

@Data
@Component
@ConfigurationProperties(prefix = "hmdp.cache.mq")
public class CacheMqProperties {

    private boolean enabled = true;
    private String exchange = "hmdp.cache.invalidate";
    private String queuePrefix = "hmdp.cache.instance";
    private String deadLetterExchange = "hmdp.cache.invalidate.dlx";
    private String deadLetterQueue = "hmdp.cache.invalidate.dlq";
    private Duration queueExpires = Duration.ofDays(1);
    private Duration confirmTimeout = Duration.ofSeconds(5);
    // 包含首次执行在内的总尝试次数。
    private int producerMaxAttempts = 3;
    private int consumerMaxAttempts = 3;
    private Duration retryInterval = Duration.ofMillis(500);

    @PostConstruct
    public void validate() {
        Assert.hasText(exchange, "exchange不能为空");
        Assert.hasText(queuePrefix, "queue-prefix不能为空");
        Assert.hasText(deadLetterExchange, "dead-letter-exchange不能为空");
        Assert.hasText(deadLetterQueue, "dead-letter-queue不能为空");
        Assert.isTrue(producerMaxAttempts > 0 && consumerMaxAttempts > 0, "消息尝试次数必须大于0");
        for (Duration duration : new Duration[]{queueExpires, confirmTimeout, retryInterval}) {
            Assert.isTrue(duration != null && duration.toMillis() > 0, "消息时间配置必须大于0毫秒");
        }
    }
}
