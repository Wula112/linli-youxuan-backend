package com.hmdp.framework.ratelimit;

import com.hmdp.common.dto.UserDTO;
import com.hmdp.common.context.UserHolder;
import lombok.RequiredArgsConstructor;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.aop.support.AopUtils;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.hmdp.common.constant.RedisConstants.RATE_LIMIT_KEY;

@Aspect
@Component
@RequiredArgsConstructor
public class RateLimiterAspect {

    private static final DefaultRedisScript<Long> SLIDING_WINDOW_SCRIPT;

    static {
        SLIDING_WINDOW_SCRIPT = new DefaultRedisScript<>();
        SLIDING_WINDOW_SCRIPT.setLocation(new ClassPathResource("limiter.lua"));
        SLIDING_WINDOW_SCRIPT.setResultType(Long.class);
    }

    private final StringRedisTemplate stringRedisTemplate;
    private final Environment environment;

    @Around("@annotation(rateLimiter)")
    public Object limit(ProceedingJoinPoint point, RateLimiter rateLimiter) throws Throwable {
        long windowMillis = DurationStyle.detectAndParse(
                environment.resolveRequiredPlaceholders(rateLimiter.window())).toMillis();
        int limit = Integer.parseInt(environment.resolveRequiredPlaceholders(rateLimiter.limit()));
        Assert.isTrue(windowMillis > 0, "限流window必须大于0毫秒");
        Assert.isTrue(limit > 0, "限流limit必须大于0");

        String key = buildRateLimitKey(point, rateLimiter);
        // 每次调用生成独立member，同毫秒、多实例请求不会因ZADD覆盖而少计数。
        Long count = stringRedisTemplate.execute(SLIDING_WINDOW_SCRIPT, List.of(key),
                Long.toString(windowMillis), Integer.toString(limit), UUID.randomUUID().toString());
        if (count == null || count < 0) {
            throw new IllegalStateException("限流脚本返回结果异常");
        }
        if (count == 0) {
            throw new RateLimitException(rateLimiter.message());
        }
        return point.proceed();
    }

    private String buildRateLimitKey(ProceedingJoinPoint point, RateLimiter rateLimiter) {
        MethodSignature signature = (MethodSignature) point.getSignature();
        Method method = AopUtils.getMostSpecificMethod(signature.getMethod(), point.getTarget().getClass());
        String prefix = environment.resolveRequiredPlaceholders(rateLimiter.key());
        Assert.hasText(prefix, "限流key不能为空");
        String parameters = Arrays.stream(method.getParameterTypes()).map(Class::getName)
                .collect(Collectors.joining(","));
        String key = RATE_LIMIT_KEY + prefix + ":" + method.getDeclaringClass().getName()
                + ":" + method.getName() + "(" + parameters + ")";
        return switch (rateLimiter.type()) {
            case METHOD -> key;
            case USER -> key + ":user:" + getCurrentUserId();
            case IP -> key + ":ip:" + getClientIp();
        };
    }

    private Long getCurrentUserId() {
        UserDTO user = UserHolder.getUser();
        if (user == null || user.getId() == null) {
            throw new RateLimitException("请先登录");
        }
        return user.getId();
    }

    private String getClientIp() {
        if (!(RequestContextHolder.getRequestAttributes() instanceof ServletRequestAttributes attributes)) {
            throw new IllegalStateException("IP限流需要HTTP请求上下文");
        }
        // 不直接信任客户端可伪造的X-Forwarded-For；代理场景先配置可信的转发处理。
        return attributes.getRequest().getRemoteAddr();
    }
}
