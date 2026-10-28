package com.hmdp.mapper;

import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;
import java.util.List;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface VoucherOrderMapper extends BaseMapper<VoucherOrder> {

    VoucherOrder selectRoutedOrder(@Param("orderId") Long orderId, @Param("userId") Long userId,
                                  @Param("voucherId") Long voucherId);

    /** 仅本人未支付且未超时的订单可以支付，影响行数为1表示本次完成状态转换。 */
    int payIfUnpaid(@Param("orderId") Long orderId, @Param("userId") Long userId,
                    @Param("voucherId") Long voucherId, @Param("payType") Integer payType);

    int closeIfExpired(@Param("orderId") Long orderId, @Param("userId") Long userId,
                       @Param("voucherId") Long voucherId);

    int markRedisStockRestored(@Param("orderId") Long orderId, @Param("userId") Long userId,
                              @Param("voucherId") Long voucherId);

    List<Long> findExpiredUnpaidOrders(@Param("afterId") long afterId, @Param("limit") int limit);

    List<Long> findClosedOrdersAwaitingStockRestore(@Param("afterId") long afterId, @Param("limit") int limit);
}
