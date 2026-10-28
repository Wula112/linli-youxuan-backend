package com.hmdp.service.impl;

import com.hmdp.common.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.mq.message.CacheInvalidationMessage;
import com.hmdp.service.ISeckillVoucherService;
import com.baomidou.mybatisplus.spring.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.transaction.annotation.Transactional;

import jakarta.annotation.Resource;

/**
 * <p>
 * 秒杀优惠券表，与优惠券是一对一关系 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2022-01-04
 */
@Service
public class SeckillVoucherServiceImpl extends ServiceImpl<SeckillVoucherMapper, SeckillVoucher> implements ISeckillVoucherService {

    @Resource
    private VoucherMapper voucherMapper;

    @Resource
    private ApplicationEventPublisher eventPublisher;

    @Override
    @Transactional
    public boolean updateById(SeckillVoucher seckillVoucher) {
        boolean success = super.updateById(seckillVoucher);
        if (success) {
            Voucher voucher = voucherMapper.selectById(seckillVoucher.getVoucherId());
            if (voucher != null) {
                eventPublisher.publishEvent(CacheInvalidationMessage.voucherList(voucher.getShopId()));
            }
        }
        return success;
    }
}
