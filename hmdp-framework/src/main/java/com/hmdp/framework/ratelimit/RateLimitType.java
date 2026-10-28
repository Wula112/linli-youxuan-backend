package com.hmdp.framework.ratelimit;

/** 限流维度均包含方法标识，避免不同接口误用同一个窗口。 */
public enum RateLimitType {
    METHOD,
    USER,
    IP
}
