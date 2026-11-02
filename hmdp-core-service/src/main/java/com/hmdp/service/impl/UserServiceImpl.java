package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.spring.service.impl.ServiceImpl;
import com.hmdp.common.dto.LoginFormDTO;
import com.hmdp.common.dto.Result;
import com.hmdp.common.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.common.constant.RedisConstants;
import com.hmdp.common.util.RegexUtils;
import com.hmdp.common.context.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.stereotype.Service;

import jakarta.servlet.http.HttpSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hmdp.common.constant.RedisConstants.*;
import static com.hmdp.common.constant.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result<Void> sendCode(String phone, HttpSession session) {
        // 1. 校验手机号
        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号格式错误！");
        }
        // 2. 生成验证码
        String code = RandomUtil.randomNumbers(6);
        // 3. 保存验证码到Redis
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone,code, Expiration.seconds(120L));
        // 4. 发送验证码
        log.debug("发送验证码成功！验证码：{}",code);

        return Result.ok();
    }

    @Override
    public Result<String> login(LoginFormDTO loginForm, HttpSession session) {
        // 1. 校验手机号
        String phone = loginForm.getPhone();
        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号格式错误！");
        }
        // 2. 校验验证码是否正确
        String codeKey = LOGIN_CODE_KEY + phone;
        String cachedCode = stringRedisTemplate.opsForValue().get(codeKey);
        String code = loginForm.getCode();
        if (cachedCode == null || !cachedCode.equals(code)) {
            return Result.fail("验证码错误！");
        }
        // 3. 验证码正确，数据库中查用户，没有就新建
        User user = query().eq("phone",phone).one();
        if(user == null){
            user = createUserWithPhone(phone);
        }
        // 4. 生成用户token,返回给用户，并存储用户信息到Redis
        String token = UUID.randomUUID().toString();
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String,Object> userMap = BeanUtil.beanToMap(userDTO,new HashMap<>(),
                CopyOptions.create().setIgnoreNullValue(true).
                        setFieldValueEditor((fieldName,filedValue) -> filedValue.toString()));
        String tokenKey = LOGIN_USER_KEY + token;

        stringRedisTemplate.opsForHash().putAll(tokenKey,userMap);
        stringRedisTemplate.expire(tokenKey,Expiration.seconds(LOGIN_USER_TTL));

        return Result.ok(token);
    }

    @Override
    public Result<Void> sign() {
        // 1. 获取当前用户信息
        String userId = UserHolder.getUser().getId().toString();
        // 2. 获取当前日期
        LocalDateTime now = LocalDateTime.now();
        String date = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        // 2.1 拿到当前日期的Offset
        int dayOfMonth = now.getDayOfMonth();
        // 3. 签到key
        String key = USER_SIGN_KEY + userId + date;
        // 4. 签到
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth - 1, true);
        return Result.ok();
    }

    @Override
    public Result<Integer> signCount() {
        // 1. 获取当前用户信息
        String userId = UserHolder.getUser().getId().toString();
        // 2. 获取当前日期
        LocalDateTime now = LocalDateTime.now();
        String date = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        // 2.1 拿到当前日期的Offset
        int dayOfMonth = now.getDayOfMonth();
        // 3. 签到key
        String key = USER_SIGN_KEY + userId + date;
        // 4. 拿到截至当月今日 的签到数据
        List<Long> signBits = stringRedisTemplate.opsForValue().bitField(key, BitFieldSubCommands.create()
                .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0));
        if(signBits == null || signBits.isEmpty()){
            return Result.ok(0);
        }
        Long sign = signBits.getFirst();
        if(sign == 0){
            return Result.ok(0);
        }
        // 5. 与1做与运算，统计连续签到天数
        int count = 0;
        while ((1 & sign) == 1) {
            count++;
            sign >>>= 1;
        }
        return Result.ok(count);
    }

    private User createUserWithPhone(String phone){
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        save(user);
        return user;
    }
}
