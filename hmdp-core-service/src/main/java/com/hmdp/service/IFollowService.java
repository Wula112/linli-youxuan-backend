package com.hmdp.service;

import com.hmdp.common.dto.Result;
import com.hmdp.common.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.baomidou.mybatisplus.spring.service.IService;

import java.util.List;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IFollowService extends IService<Follow> {

    Result<Void> follow(Long followUserId, Boolean isFollow);

    Result<Boolean> isFollow(Long followUserId);

    Result<List<UserDTO>> commonFollow(Long id);
}
