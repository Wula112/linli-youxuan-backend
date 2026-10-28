package com.hmdp.framework.ratelimit;

import java.lang.annotation.*;

/** Redis滑动窗口限流，应用于Spring代理调用的方法。 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RateLimiter {

    String key() default "api";

    /** 支持1s、500ms等Duration格式，以及${...}配置占位符。 */
    String window() default "1s";

    /** 支持配置占位符，必须为正整数。 */
    String limit() default "100";

    String message() default "请求过于频繁，请稍后重试";

    RateLimitType type() default RateLimitType.METHOD;
}
