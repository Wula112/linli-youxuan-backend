package com.hmdp.service;

import com.baomidou.mybatisplus.spring.service.IService;
import com.hmdp.common.dto.LoginFormDTO;
import com.hmdp.common.dto.Result;
import com.hmdp.entity.User;

import jakarta.servlet.http.HttpSession;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IUserService extends IService<User> {
    Result<Void> sendCode(String phone, HttpSession session);
    Result<String> login(LoginFormDTO loginForm, HttpSession session);

    Result<Void> sign();

    Result<Integer> signCount();
}
