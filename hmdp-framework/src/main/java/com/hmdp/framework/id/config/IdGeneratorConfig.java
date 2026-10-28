package com.hmdp.framework.id.config;

import com.baomidou.mybatisplus.core.incrementer.DefaultIdentifierGenerator;
import com.baomidou.mybatisplus.core.incrementer.IdentifierGenerator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class IdGeneratorConfig {

    /** MyBatis-Plus 的雪花实现：时间戳 + 数据中心/机器编号 + 毫秒内序列。 */
    @Bean
    public IdentifierGenerator identifierGenerator(
            @Value("${hmdp.id.worker-id:0}") long workerId,
            @Value("${hmdp.id.datacenter-id:0}") long datacenterId) {
        return new DefaultIdentifierGenerator(workerId, datacenterId);
    }
}
