package com.hmdp.service.impl;

import com.hmdp.common.enums.OrderProcessStatus;
import com.hmdp.common.enums.VoucherOrderStatus;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.entity.VoucherOrderProcess;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mapper.VoucherOrderProcessMapper;
import com.hmdp.mq.message.VoucherOrderMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;

/** 所有建单、死信登记和补偿决策使用同一条数据库记录仲裁。 */
@Service
@RequiredArgsConstructor
public class VoucherOrderProcessService {
    private final VoucherOrderProcessMapper mapper;
    private final VoucherOrderMapper orders;

    /** 调用方必须持有数据库事务，并在事务结束前完成相关订单写入。 */
    @Transactional(propagation = Propagation.MANDATORY)
    public VoucherOrderProcess lock(Long orderId, Long userId, Long voucherId) {
        mapper.insertIfAbsent(orderId, userId, voucherId);
        VoucherOrderProcess process = mapper.selectForUpdate(orderId);
        if (process == null || !Objects.equals(process.getUserId(), userId)
                || !Objects.equals(process.getVoucherId(), voucherId)) {
            throw new IllegalStateException("订单处理记录标识不匹配，orderId=" + orderId);
        }
        return process;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void transition(VoucherOrderProcess process, OrderProcessStatus target) {
        if (mapper.transition(process.getOrderId(), process.getStatus(), target.name()) != 1) {
            throw new IllegalStateException("订单处理状态已改变，orderId=" + process.getOrderId());
        }
        process.setStatus(target.name());
    }

    /** 死信只标记待对账；已建单或已开始补偿的记录不能被覆盖。 */
    @Transactional(rollbackFor = Exception.class)
    public void recordDeadLetter(VoucherOrderMessage message) {
        VoucherOrderProcess process = lock(message.orderId(), message.userId(), message.voucherId());
        if (OrderProcessStatus.PENDING.name().equals(process.getStatus())) {
            if (!markExistingOrder(process)) transition(process, OrderProcessStatus.DEAD_LETTER);
        }
    }

    /** 事务提交后才允许执行 Redis 回补。已建单时返回 CREATED。 */
    @Transactional(rollbackFor = Exception.class)
    public OrderProcessStatus requestCompensation(Long orderId, Long userId, Long voucherId) {
        VoucherOrderProcess process = lock(orderId, userId, voucherId);
        OrderProcessStatus current = OrderProcessStatus.valueOf(process.getStatus());
        if (current == OrderProcessStatus.PENDING || current == OrderProcessStatus.DEAD_LETTER) {
            if (markExistingOrder(process)) return OrderProcessStatus.CREATED;
            transition(process, OrderProcessStatus.COMPENSATING);
            return OrderProcessStatus.COMPENSATING;
        }
        return current;
    }

    /** 升级期历史订单可能尚未有处理记录；绝不能将已建单订单当作缺失订单回补。 */
    private boolean markExistingOrder(VoucherOrderProcess process) {
        VoucherOrder order = orders.selectRoutedOrder(process.getOrderId(), process.getUserId(), process.getVoucherId());
        if (order == null) return false;
        if (Integer.valueOf(VoucherOrderStatus.CLOSED.getCode()).equals(order.getStatus())) {
            throw new IllegalStateException("已关闭订单缺少正确处理状态，orderId=" + process.getOrderId());
        }
        transition(process, OrderProcessStatus.CREATED);
        return true;
    }

    @Transactional(rollbackFor = Exception.class)
    public void markCompensated(Long orderId) {
        if (mapper.transition(orderId, OrderProcessStatus.COMPENSATING.name(), OrderProcessStatus.COMPENSATED.name()) == 0) {
            VoucherOrderProcess current = mapper.selectById(orderId);
            if (current == null || !OrderProcessStatus.COMPENSATED.name().equals(current.getStatus())) {
                throw new IllegalStateException("补偿状态更新失败，orderId=" + orderId);
            }
        }
    }
}
