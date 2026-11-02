package com.hmdp.service.impl;

import com.hmdp.common.context.UserHolder;
import com.hmdp.common.dto.Result;
import com.hmdp.common.enums.VoucherOrderStatus;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.entity.VoucherOrderRouter;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mapper.VoucherOrderRouterMapper;
import jakarta.annotation.Resource;
import org.springframework.stereotype.Service;

/**
 * 模拟支付：由单条条件 UPDATE 完成状态迁移，不额外开启事务。
 *
 * <p>{@link VoucherOrderServiceImpl} 的支付入口委托到这里，与关单共享同一条
 * “状态 + 归属 + 到期时间”条件更新，二者竞争同一笔订单时只有一方生效。
 */
@Service
public class VoucherOrderPaymentService {

    @Resource
    private VoucherOrderRouterMapper voucherOrderRouterMapper;

    @Resource
    private VoucherOrderMapper voucherOrderMapper;

    public Result<Long> mockPay(Long orderId, Integer payType) {
        if (orderId == null || orderId <= 0) {
            return Result.fail("订单号不正确");
        }
        if (payType == null || payType < 1 || payType > 3) {
            return Result.fail("支付方式不正确");
        }
        if (UserHolder.getUser() == null) {
            return Result.fail("请先登录");
        }
        Long userId = UserHolder.getUser().getId();
        // 状态、归属和到期时间在同一条UPDATE中判断，数据库时间决定是否允许支付。
        VoucherOrderRouter route = voucherOrderRouterMapper.selectById(orderId);
        if (route == null || !userId.equals(route.getUserId())) {
            return Result.fail("订单不存在");
        }
        if (voucherOrderMapper.payIfUnpaid(orderId, userId, route.getVoucherId(), payType) == 1) {
            return Result.ok(orderId);
        }
        VoucherOrder order = voucherOrderMapper.selectRoutedOrder(orderId, route.getUserId(), route.getVoucherId());
        if (order == null || !userId.equals(order.getUserId())) {
            return Result.fail("订单不存在");
        }
        if (Integer.valueOf(VoucherOrderStatus.PAID.getCode()).equals(order.getStatus())) {
            // 重复支付按成功返回，不覆盖首次支付方式和支付时间。
            return Result.ok(orderId);
        }
        if (Integer.valueOf(VoucherOrderStatus.CLOSED.getCode()).equals(order.getStatus())) {
            return Result.fail("订单已关闭，无法支付");
        }
        if (Integer.valueOf(VoucherOrderStatus.UNPAID.getCode()).equals(order.getStatus())) {
            return Result.fail("订单已超时，无法支付");
        }
        return Result.fail("当前订单状态不支持支付");
    }
}
