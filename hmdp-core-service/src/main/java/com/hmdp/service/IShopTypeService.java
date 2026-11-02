package com.hmdp.service;

import com.hmdp.common.dto.Result;
import com.hmdp.entity.ShopType;
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
public interface IShopTypeService extends IService<ShopType> {
    Result<List<ShopType>> queryTypeList();
}
