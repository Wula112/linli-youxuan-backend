package com.hmdp.interceptor;

import com.hmdp.common.constant.RedisConstants;
import com.hmdp.common.context.UserHolder;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.common.dto.UserDTO;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.Map;

import static com.hmdp.common.constant.RedisConstants.LOGIN_USER_KEY;
import static com.hmdp.common.constant.RedisConstants.LOGIN_USER_TTL;

public class RefreshTokenInterceptors implements HandlerInterceptor {

    private final StringRedisTemplate stringRedisTemplate;

    public RefreshTokenInterceptors(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, @Nullable Exception ex) throws Exception {
        UserHolder.removeUser();
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // 1. 获取请求头中的token
        String token = request.getHeader("authorization");
        if(StrUtil.isBlank(token)){
            return true;
        }

        // 2. 获取Redis中的用户
        String tokenKey = LOGIN_USER_KEY + token;
        Map<Object,Object> userMap = stringRedisTemplate.opsForHash().entries(tokenKey);
        if(userMap.isEmpty()){
            return true;
        }
        // 3.将获取到的Hash数据转为UserDTO对象
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
        UserHolder.saveUser(userDTO);

        // 4.刷新token有效期
        stringRedisTemplate.expire(tokenKey, Expiration.seconds(LOGIN_USER_TTL));
        return true;
    }
}
