package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.spring.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import jakarta.annotation.Resource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IUserService userService;

    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        Long userId = UserHolder.getUser().getId();
        // 1. 关注
        String key = "follows:" + userId;
        if(isFollow){
            Follow follow = new Follow();
            follow.setFollowUserId(followUserId);
            follow.setUserId(userId);
            boolean success = save(follow);
            if(success){
                // 关注成功，放入Redis
                stringRedisTemplate.opsForSet().add(key, followUserId.toString());
            }
        } else {
            // 2. 关注了，取关
            boolean success = remove(new QueryWrapper<Follow>().
                    eq("user_id", userId).
                    eq("follow_user_id", followUserId));
            if(success){
                stringRedisTemplate.opsForSet().remove(key, followUserId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result isFollow(Long followUserId) {
        Long userId = UserHolder.getUser().getId();
        // 查询是否关注
        Long count = query().eq("user_id", userId).
                eq("follow_user_id", followUserId).count();

        return Result.ok(count > 0);
    }

    @Override
    public Result commonFollow(Long id) {
        String userId = UserHolder.getUser().getId().toString();
        String key = "follows:" + userId;
        String key2 = "follows:" + id;
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key, key2);
        if(intersect == null || intersect.isEmpty()){
            return Result.ok(Collections.emptyList());
        }

        List<Long> ids = intersect.stream().map(Long::valueOf)
                .toList();
        List<UserDTO> users = userService.listByIds(ids)
                .stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .toList();

        return Result.ok(users);
    }
}
