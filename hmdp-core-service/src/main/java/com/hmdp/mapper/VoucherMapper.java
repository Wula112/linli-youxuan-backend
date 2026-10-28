package com.hmdp.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hmdp.entity.Voucher;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.time.LocalDateTime;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface VoucherMapper extends BaseMapper<Voucher> {

    default List<Voucher> queryVoucherOfShop(Long shopId) {
        List<Long> ids = queryVoucherIdsOfShop(shopId);
        return ids.isEmpty() ? List.of() : queryVoucherByIds(ids);
    }

    @Select("SELECT voucher_id FROM tb_shop_voucher_index WHERE shop_id = #{shopId} ORDER BY voucher_id")
    List<Long> queryVoucherIdsOfShop(@Param("shopId") Long shopId);

    List<Voucher> queryVoucherByIds(@Param("ids") List<Long> ids);

    /** 自动预热候选：上架的秒杀券，活动未结束且已开始或即将开始；按商铺去重并限制数量。 */
    List<Long> findWarmupShopIds(@Param("now") LocalDateTime now,
                                @Param("startsBefore") LocalDateTime startsBefore,
                                @Param("limit") int limit);

    @Select("SELECT * FROM tb_voucher WHERE id = #{id} FOR UPDATE")
    Voucher selectForUpdate(@Param("id") Long id);
}
