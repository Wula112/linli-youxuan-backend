package com.hmdp.mq.config;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.time.Duration;

/** RocketMQ 客户端配置：地址、生产重试与消费重试。 */
@Data
@Component
@ConfigurationProperties(prefix = "hmdp.rocketmq")
public class RocketMqProperties {

    private String nameServer = "127.0.0.1:9876";
    private String producerGroup = "hmdp-producer";
    private Duration sendTimeout = Duration.ofSeconds(3);
    /** 同步发送失败时的客户端重试次数，与事务回查无关。 */
    private int retryTimesWhenSendFailed = 2;
    /** 消费失败后由 Broker 重投的次数，超过后进入 %DLQ%<consumer-group>。 */
    private int maxReconsumeTimes = 2;
    private int consumeThreads = 4;
    /** Broker 事务回查的处理线程数。 */
    private int transactionCheckThreads = 4;

    @PostConstruct
    public void validate() {
        Assert.hasText(nameServer, "name-server不能为空");
        Assert.hasText(producerGroup, "producer-group不能为空");
        Assert.isTrue(sendTimeout != null && sendTimeout.toMillis() > 0, "send-timeout必须大于0毫秒");
        Assert.isTrue(retryTimesWhenSendFailed >= 0, "retry-times-when-send-failed不能为负");
        Assert.isTrue(maxReconsumeTimes >= 0, "max-reconsume-times不能为负");
        Assert.isTrue(consumeThreads > 0, "consume-threads必须大于0");
        Assert.isTrue(transactionCheckThreads > 0, "transaction-check-threads必须大于0");
    }
}
