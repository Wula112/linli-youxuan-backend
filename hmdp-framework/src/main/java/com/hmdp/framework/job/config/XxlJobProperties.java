package com.hmdp.framework.job.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.Assert;

@Data
@ConfigurationProperties(prefix = "xxl.job")
public class XxlJobProperties {
    private String adminAddresses;
    private String accessToken;
    private String appname = "hmdp-job-executor";
    private String address = "";
    private String ip = "";
    private int port = 9999;
    private int timeout = 3;
    private String logPath = "./logs/xxl-job";
    private int logRetentionDays = 14;

    public void validate() {
        Assert.hasText(adminAddresses, "XXL-JOB admin-addresses不能为空");
        Assert.hasText(accessToken, "XXL-JOB access-token不能为空，须与Admin一致");
        Assert.hasText(appname, "XXL-JOB appname不能为空");
        Assert.hasText(logPath, "XXL-JOB log-path不能为空");
        Assert.isTrue(port > 0 && port <= 65535, "XXL-JOB port必须在1到65535之间");
        Assert.isTrue(timeout >= 1 && timeout <= 10, "XXL-JOB timeout必须在1到10秒之间");
        Assert.isTrue(logRetentionDays >= 3, "XXL-JOB log-retention-days不能小于3");
    }
}
