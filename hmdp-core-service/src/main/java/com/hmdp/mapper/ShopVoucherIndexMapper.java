package com.hmdp.mapper;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;

/** 只保存店铺与券的关联，状态和库存仍读取券主表与库存表。 */
public interface ShopVoucherIndexMapper {
    @Insert("INSERT INTO tb_shop_voucher_index(shop_id, voucher_id) VALUES(#{shopId}, #{voucherId})")
    int insert(@Param("shopId") Long shopId, @Param("voucherId") Long voucherId);

    @Delete("DELETE FROM tb_shop_voucher_index WHERE shop_id = #{shopId} AND voucher_id = #{voucherId}")
    int delete(@Param("shopId") Long shopId, @Param("voucherId") Long voucherId);
}
