package com.hmdp.config;

import org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.module.SimpleModule;
import tools.jackson.databind.ser.std.ToStringSerializer;

@Configuration
public class IdJsonConfig {
    /** 雪花 ID 超过 JavaScript 安全整数范围，HTTP JSON 中以字符串传输 Long。 */
    @Bean
    public JsonMapperBuilderCustomizer longAsString() {
        return builder -> builder.addModule(new SimpleModule("long-as-string")
                .addSerializer(Long.class, ToStringSerializer.instance)
                .addSerializer(Long.TYPE, ToStringSerializer.instance));
    }
}
