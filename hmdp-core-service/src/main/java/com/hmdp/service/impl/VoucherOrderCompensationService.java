package com.hmdp.service.impl;

import com.hmdp.common.enums.OrderProcessStatus;
import com.hmdp.entity.VoucherOrderProcess;
import com.hmdp.mapper.VoucherOrderProcessMapper;
import com.hmdp.repository.redis.SeckillReservation;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

/** 按已提交的补偿状态释放预占；Redis 凭证使超时或进程退出后的重试安全。 */
@Service
@RequiredArgsConstructor
public class VoucherOrderCompensationService {
    private final VoucherOrderProcessMapper mapper;
    private final VoucherOrderProcessService processService;
    private final SeckillReservation reservation;

    public void compensate(Long orderId) {
        VoucherOrderProcess process = mapper.selectById(orderId);
        if (process == null || !OrderProcessStatus.COMPENSATING.name().equals(process.getStatus())) {
            return;
        }
        Long result = reservation.rollback(process.getVoucherId(), process.getUserId(), orderId);
        if (result == null || (result != 1L && result != 2L)) {
            throw new IllegalStateException("预占释放结果无法确认，orderId=" + orderId + "，result=" + result);
        }
        processService.markCompensated(orderId);
    }
}
