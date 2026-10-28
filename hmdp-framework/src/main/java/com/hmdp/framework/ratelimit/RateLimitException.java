package com.hmdp.framework.ratelimit;

/** 预期的限流拒绝，由现有统一异常处理转换为Result.fail，不记录异常堆栈。 */
public class RateLimitException extends RuntimeException {

    public RateLimitException(String message) {
        super(message);
    }
}
