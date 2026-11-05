package com.hmdp.service.impl;

import com.hmdp.common.enums.OrderProcessStatus;
import com.hmdp.common.enums.VoucherOrderStatus;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.entity.VoucherOrderProcess;
import com.hmdp.entity.VoucherOrderRouter;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mapper.VoucherOrderRouterMapper;
import com.hmdp.service.ISeckillVoucherService;
import jakarta.annotation.Resource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/** 条件关单、返还数据库库存和登记 Redis 补偿在同一个事务内完成。 */
@Service
public class VoucherOrderCloseService {
    @Resource private VoucherOrderRouterMapper voucherOrderRouterMapper;
    @Resource private VoucherOrderMapper voucherOrderMapper;
    @Resource private ISeckillVoucherService seckillVoucherService;
    @Resource private VoucherOrderProcessService processService;
    @Resource private PlatformTransactionManager transactionManager;

    public void closeExpiredOrder(Long orderId) {
        if (orderId == null || orderId <= 0) {
            throw new IllegalArgumentException("订单号不正确");
        }
        VoucherOrderRouter route = voucherOrderRouterMapper.selectById(orderId);
        if (route == null) return;
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        transaction.executeWithoutResult(ignored -> {
            VoucherOrderProcess process = processService.lock(orderId, route.getUserId(), route.getVoucherId());
            OrderProcessStatus status = OrderProcessStatus.valueOf(process.getStatus());
            if (status == OrderProcessStatus.COMPENSATING || status == OrderProcessStatus.COMPENSATED) return;
            if (status != OrderProcessStatus.CREATED) {
                // 存量订单切换时也要先确认真实订单，不能凭不存在的处理记录直接返库。
                VoucherOrder order = voucherOrderMapper.selectRoutedOrder(orderId, route.getUserId(), route.getVoucherId());
                if (order == null) return;
                if (Integer.valueOf(VoucherOrderStatus.CLOSED.getCode()).equals(order.getStatus())) {
                    throw new IllegalStateException("已关闭订单缺少正确处理状态，orderId=" + orderId);
                }
                processService.transition(process, OrderProcessStatus.CREATED);
            }
            int updated = voucherOrderMapper.closeIfExpired(orderId, route.getUserId(), route.getVoucherId());
            if (updated == 0) return;
            if (!seckillVoucherService.update().setSql("stock = stock + 1")
                    .eq("voucher_id", route.getVoucherId()).update()) {
                throw new IllegalStateException("关单返还数据库库存失败，orderId=" + orderId);
            }
            processService.transition(process, OrderProcessStatus.COMPENSATING);
        });
    }
}
