package com.hmdp.job.config;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.time.Duration;

@Data
@Component
@ConfigurationProperties(prefix = "hmdp.job")
public class JobProperties {
    public enum Scheduler { LOCAL, XXL }

    // 本机默认沿用原调度；启用xxl后，两个本地定时入口一起关闭。
    private Scheduler scheduler = Scheduler.LOCAL;
    // 软预算：到时不再领取新记录，不强行打断正在提交的业务事务。
    private Duration maxRunTime = Duration.ofSeconds(30);

    @PostConstruct
    public void validate() {
        Assert.notNull(scheduler, "scheduler只能是local或xxl");
        Assert.isTrue(maxRunTime != null && maxRunTime.toMillis() > 0,
                "max-run-time必须大于0毫秒");
    }
}
