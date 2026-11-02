package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.common.dto.Result;
import com.hmdp.common.dto.ScrollResult;
import com.hmdp.common.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.spring.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.common.constant.SystemConstants;
import com.hmdp.common.context.UserHolder;
import jakarta.annotation.Resource;
import org.jspecify.annotations.NonNull;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

import static com.hmdp.common.constant.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.common.constant.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IFollowService followService;

    @Override
    public Result<List<Blog>> queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.queryLikeBlog(blog);
            this.queryBlogUser(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result<List<Blog>> queryMyBlog(Integer current) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        // 根据用户查询
        Page<Blog> page = query()
                .eq("user_id", user.getId()).page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 返回当前页数据
        return Result.ok(page.getRecords());
    }

    @Override
    public Result<List<Blog>> queryBlogByUserId(Long userId, Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .eq("user_id", userId).page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 返回当前页数据
        return Result.ok(page.getRecords());
    }

    @Override
    public Result<Blog> queryBlogById(Long blogId) {
        // 1.根据ID 查博客
        Blog blog = getById(blogId);
        if(blog == null){
            return Result.fail("博客不存在!");
        }
        // 2. 查询用户信息并封装返回
        queryBlogUser(blog);
        queryLikeBlog(blog);
        return Result.ok(blog);
    }

    // 判断是否点赞过该博客
    private void queryLikeBlog(Blog blog){
        UserDTO user = UserHolder.getUser();
        if(user == null){
            // 用户没登陆，不用查是否点赞博客
            return;
        }
        String userId = user.getId().toString();
        String key = BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId);
        blog.setIsLike(score != null);
    }

    @Override
    public Result<Void> likeBlog(Long id) {
        String key = BLOG_LIKED_KEY + id;
        // 1. 判断是否已点过赞
        String userId = UserHolder.getUser().getId().toString();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId);
        if(score != null){
            // 2.点过赞，取消赞
            boolean success = update().setSql("liked = liked - 1").eq("id", id).update();
            if(success){
                stringRedisTemplate.opsForZSet().remove(key, userId);
            }
        }
        else{
            // 3.没点过赞
            boolean success = update().setSql("liked = liked + 1").eq("id", id).update();
            if(success){
                stringRedisTemplate.opsForZSet().add(key, userId, System.currentTimeMillis());
            }
        }

        return Result.ok();
    }

    @Override
    public Result<List<UserDTO>> queryBlogLikes(Long id) {
        // 1. 查询点赞该博客时间 Top5 的用户
        String key = BLOG_LIKED_KEY + id;
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if(top5 == null || top5.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        // 2. 解析出用户Id
        List<Long> ids = top5.stream().map(Long::valueOf).toList();
        String join = StrUtil.join(",", ids);
        // 3. 根据ID查询用户信息
        List<UserDTO> userDTOS = userService.query()
                .in("id", ids)
                .last("ORDER BY FIELD(id," + join + ")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .toList();

        // 4.返回
        return Result.ok(userDTOS);
    }

    @Override
    public Result<Long> saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        save(blog);
        // 发送博文到粉丝收件箱
        // 1. 获取所有粉丝
        List<Follow> fans = followService.query().eq("follow_user_id", user.getId()).list();
        // 2. 推送到收件箱
        for (Follow fan : fans) {
            // 粉丝ID
            Long userId = fan.getUserId();
            String key = FEED_KEY + userId;
            // 添加博客ID 到粉丝收件箱
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }
        // 返回id
        return Result.ok(blog.getId());

    }

    @Override
    public Result<Object> queryFollowBlog(Long lastId, Integer offset) {
        // 1. 从收件箱中查博客
        Long userId = UserHolder.getUser().getId();
        String key = FEED_KEY + userId;
        Set<ZSetOperations.@NonNull TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet().reverseRangeByScoreWithScores(key, 0, lastId, offset, 2);
        if(typedTuples == null || typedTuples.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        // 2. 解析博客ID 最小时间戳 offset
        List<Long> blogIds = new ArrayList<>(typedTuples.size());
        long mintime = 0;
        int os = 1;
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            // 获取博客ID
            String value = typedTuple.getValue();
            if (value != null) {
                blogIds.add(Long.valueOf(value));
            }
            long time = Objects.requireNonNull(typedTuple.getScore()).longValue();
            // 拿到本次查询的最小时间戳
            if(time == mintime){
                os++;
            }else {
                mintime = time;
                os = 1;
            }

        }
        // 3. 根据博客ID 查找博客
        String ids = StrUtil.join(",", blogIds);
        List<Blog> blogList = query().in("id", blogIds).last("ORDER BY FIELD(id," + ids + ")").list();
        // 4. 然后需要查每个Blog对应的用户和点赞信息
        for (Blog blog : blogList) {
            queryBlogUser(blog);
            queryLikeBlog(blog);
        }
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogList);
        scrollResult.setOffset(os);
        scrollResult.setMinTime(mintime);
        return Result.ok(scrollResult);
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
