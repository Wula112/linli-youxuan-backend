package com.hmdp.service;

import com.hmdp.common.dto.Result;
import com.hmdp.entity.Shop;
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
public interface IShopService extends IService<Shop> {

    Result<Shop> queryById(Long id);

    Result<Void> update(Shop shop);

    Result<List<Shop>> queryShopByType(Integer typeId, Integer current, Double x, Double y);

    /** 按名称模糊分页查询；分页大小与排序留在业务层。 */
    Result<List<Shop>> queryShopByName(String name, Integer current);
}
