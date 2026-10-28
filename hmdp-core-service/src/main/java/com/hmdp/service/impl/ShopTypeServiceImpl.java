package com.hmdp.service.impl;

import com.hmdp.common.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.mq.message.CacheInvalidationMessage;
import com.hmdp.service.IShopTypeService;
import com.hmdp.cache.ShopTypeCacheService;
import com.baomidou.mybatisplus.spring.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.transaction.annotation.Transactional;

import jakarta.annotation.Resource;
import java.util.List;
import java.io.Serializable;

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
    @Resource
    private ShopTypeCacheService shopTypeCacheService;

    @Resource
    private ApplicationEventPublisher eventPublisher;

    @Override
    @Transactional
    public boolean save(ShopType shopType) {
        boolean success = super.save(shopType);
        if (success) {
            eventPublisher.publishEvent(CacheInvalidationMessage.shopType());
        }
        return success;
    }

    @Override
    @Transactional
    public boolean updateById(ShopType shopType) {
        boolean success = super.updateById(shopType);
        if (success) {
            eventPublisher.publishEvent(CacheInvalidationMessage.shopType());
        }
        return success;
    }

    @Override
    @Transactional
    public boolean removeById(Serializable id) {
        boolean success = super.removeById(id);
        if (success) {
            eventPublisher.publishEvent(CacheInvalidationMessage.shopType());
        }
        return success;
    }

    @Override
    public Result queryTypeList() {
        // 按配置选择MySQL、Redis或Caffeine查询路径
        List<ShopType> typeList = shopTypeCacheService.getTypeList();
        return Result.ok(typeList);
    }

}
