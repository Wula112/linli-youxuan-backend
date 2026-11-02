package com.hmdp.controller;


import com.hmdp.common.dto.Result;
import com.hmdp.common.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.service.IBlogService;
import org.springframework.web.bind.annotation.*;

import jakarta.annotation.Resource;
import java.util.List;

/**
 * <p>
 * 前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/blog")
public class BlogController {

    @Resource
    private IBlogService blogService;


    @PostMapping
    public Result<Long> saveBlog(@RequestBody Blog blog) {
        return blogService.saveBlog(blog);
    }

    @PutMapping("/like/{id}")
    public Result<Void> likeBlog(@PathVariable("id") Long id) {
        return blogService.likeBlog(id);
    }

    @GetMapping("/likes/{id}")
    public Result<List<UserDTO>> queryBlogLikes(@PathVariable("id") Long id) {
        return blogService.queryBlogLikes(id);
    }

    @GetMapping("/of/me")
    public Result<List<Blog>> queryMyBlog(@RequestParam(value = "current", defaultValue = "1") Integer current) {
        // 登录用户与分页条件都在业务层处理
        return blogService.queryMyBlog(current);
    }

    @GetMapping("/hot")
    public Result<List<Blog>> queryHotBlog(@RequestParam(value = "current", defaultValue = "1") Integer current) {
       return blogService.queryHotBlog(current);
    }

    @GetMapping("/{id}")
    public Result<Blog> queryBlogById(@PathVariable("id") Long blogId){
        return blogService.queryBlogById(blogId);
    }

    @GetMapping("/of/user")
    public Result<List<Blog>> queryBlogByUserId(
            @RequestParam(value = "current", defaultValue = "1") Integer current,
            @RequestParam("id") Long id) {
        // 分页条件由业务层维护
        return blogService.queryBlogByUserId(id, current);
    }

    @GetMapping("/of/follow")
    public Result<Object> queryFollowBlog(
            @RequestParam(value = "lastId") Long lastId,
            @RequestParam(value = "offset", defaultValue = "0") Integer offset) {
        // 根据用户查询
        return blogService.queryFollowBlog(lastId, offset);
    }



}
