package com.hmdp.service;

import com.hmdp.common.dto.Result;
import com.hmdp.common.dto.ScrollResult;
import com.hmdp.common.dto.UserDTO;
import com.hmdp.entity.Blog;
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
public interface IBlogService extends IService<Blog> {

    Result<List<Blog>> queryHotBlog(Integer current);

    Result<Blog> queryBlogById(Long blogId);

    Result<Void> likeBlog(Long id);

    Result<List<UserDTO>> queryBlogLikes(Long id);

    Result<Long> saveBlog(Blog blog);

    /**
     * 滚动分页结果：空结果沿用历史约定返回空数组，非空返回 {@link ScrollResult}，
     * 因此 data 类型保持 Object，避免改变前端已有的响应结构。
     */
    Result<Object> queryFollowBlog(Long lastId, Integer offset);

    /** 当前登录用户的博文分页；登录校验由拦截器负责。 */
    Result<List<Blog>> queryMyBlog(Integer current);

    /** 指定用户的博文分页。 */
    Result<List<Blog>> queryBlogByUserId(Long userId, Integer current);
}
