package com.hmdp.service;

import com.hmdp.common.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.spring.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result secKillVoucher(Long voucherId);

    void createVoucherOrder(VoucherOrder voucherOrder);

    /** 查询当前登录用户的订单。 */
    Result queryVoucherOrder(Long orderId);

    /** 模拟支付，不对接支付渠道、不实际扣款。 */
    Result mockPay(Long orderId, Integer payType);

    /** 超时条件关单；MySQL提交后幂等返还Redis库存并释放本订单购买资格。 */
    void closeExpiredOrder(Long orderId);
}
