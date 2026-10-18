package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.spring.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    private final StringRedisTemplate stringRedisTemplate;

    public ShopTypeServiceImpl(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public Result queryTypeList() {
        String key = RedisConstants.CACHE_SHOP_TYPE_KEY;
        // 1. 从redis查询缓存
        String typeJson = stringRedisTemplate.opsForValue().get(key);
        // 2. 缓存命中，直接返回
        if(StrUtil.isNotBlank(typeJson)){
            List<ShopType> typeList = JSONUtil.toList(typeJson, ShopType.class);
            return Result.ok(typeList);
        }
        // 3. 缓存未命中，按sort排序查询数据库
        List<ShopType> typeList = query().orderByAsc("sort").list();
        // 4. 写入Redis缓存
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(typeList));
        // 5. 返回数据
        return Result.ok(typeList);
    }

}
