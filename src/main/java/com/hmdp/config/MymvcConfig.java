package com.hmdp.config;

import com.hmdp.utils.LoginInterceptors;
import com.hmdp.utils.RefreshTokenInterceptors;
import jakarta.annotation.Resource;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class MymvcConfig implements WebMvcConfigurer {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new LoginInterceptors())
                .excludePathPatterns(
                        "/user/login",
                        "/user/code",
                        "/shop/**",
                        "/voucher/**",
                        "/shop-type/**",
                        "/blog/hot",
                        "/upload/**"
                ).order(1);
        registry.addInterceptor(new RefreshTokenInterceptors(stringRedisTemplate)).addPathPatterns("/**").order(0);
    }
}
