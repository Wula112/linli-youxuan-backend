package com.hmdp.controller;


import com.hmdp.common.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.framework.ratelimit.RateLimiter;
import com.hmdp.service.IVoucherOrderService;
import jakarta.annotation.Resource;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @PostMapping("seckill/{id}")
    @RateLimiter(key = "seckill", window = "${hmdp.seckill.rate-limit.window:1s}",
            limit = "${hmdp.seckill.rate-limit.limit:100}", message = "抢购人数过多，请稍后重试")
    public Result<Long> seckillVoucher(@PathVariable("id") Long voucherId) {
        return voucherOrderService.secKillVoucher(voucherId);
    }

    @GetMapping("/{id}")
    public Result<VoucherOrder> queryVoucherOrder(@PathVariable("id") Long orderId) {
        return voucherOrderService.queryVoucherOrder(orderId);
    }

    /** 模拟本人订单支付，payType沿用原定义：1余额、2支付宝、3微信。 */
    @PostMapping("/pay/mock/{id}")
    public Result<Long> mockPay(@PathVariable("id") Long orderId,
                          @RequestParam(value = "payType", defaultValue = "1") Integer payType) {
        return voucherOrderService.mockPay(orderId, payType);
    }
}
