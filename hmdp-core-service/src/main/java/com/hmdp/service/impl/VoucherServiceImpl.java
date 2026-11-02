package com.hmdp.service.impl;

import com.baomidou.mybatisplus.spring.service.impl.ServiceImpl;
import com.hmdp.common.dto.Result;
import com.hmdp.entity.Voucher;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.mapper.ShopVoucherIndexMapper;
import com.hmdp.mq.message.CacheInvalidationMessage;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherService;
import com.hmdp.cache.VoucherListCacheService;
import com.hmdp.repository.redis.SeckillReservation;
import org.springframework.stereotype.Service;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.transaction.annotation.Transactional;

import jakarta.annotation.Resource;
import java.util.List;
import java.io.Serializable;
import java.util.Objects;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherServiceImpl extends ServiceImpl<VoucherMapper, Voucher> implements IVoucherService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private SeckillReservation seckillReservation;

    @Resource
    private VoucherListCacheService voucherListCacheService;

    @Resource
    private ApplicationEventPublisher eventPublisher;
    @Resource
    private ShopVoucherIndexMapper shopVoucherIndexMapper;

    @Override
    @Transactional
    public boolean save(Voucher voucher) {
        boolean success = super.save(voucher);
        if (success) {
            if (shopVoucherIndexMapper.insert(voucher.getShopId(), voucher.getId()) != 1) {
                throw new IllegalStateException("保存店铺优惠券索引失败");
            }
            eventPublisher.publishEvent(CacheInvalidationMessage.voucherList(voucher.getShopId()));
        }
        return success;
    }

    @Override
    @Transactional
    public boolean updateById(Voucher voucher) {
        // 串行维护同一张券的归属，避免并发换店后遗留旧店铺索引。
        Voucher oldVoucher = baseMapper.selectForUpdate(voucher.getId());
        boolean success = super.updateById(voucher);
        if (success && oldVoucher != null) {
            eventPublisher.publishEvent(CacheInvalidationMessage.voucherList(oldVoucher.getShopId()));
            // 更换所属商铺时，新旧商铺的优惠券列表都失效。
            if (voucher.getShopId() != null && !Objects.equals(oldVoucher.getShopId(), voucher.getShopId())) {
                shopVoucherIndexMapper.delete(oldVoucher.getShopId(), voucher.getId());
                if (shopVoucherIndexMapper.insert(voucher.getShopId(), voucher.getId()) != 1) {
                    throw new IllegalStateException("更新店铺优惠券索引失败");
                }
                eventPublisher.publishEvent(CacheInvalidationMessage.voucherList(voucher.getShopId()));
            }
        }
        return success;
    }

    @Override
    @Transactional
    public boolean removeById(Serializable id) {
        Voucher voucher = baseMapper.selectForUpdate(Long.valueOf(id.toString()));
        boolean success = super.removeById(id);
        if (success && voucher != null) {
            shopVoucherIndexMapper.delete(voucher.getShopId(), voucher.getId());
            eventPublisher.publishEvent(CacheInvalidationMessage.voucherList(voucher.getShopId()));
        }
        return success;
    }

    @Override
    public Result<List<Voucher>> queryVoucherOfShop(Long shopId) {
        // 按配置选择MySQL、Redis或Caffeine查询路径
        List<Voucher> vouchers = voucherListCacheService.getVoucherByShopId(shopId);
        return Result.ok(vouchers);
    }

    @Override
    @Transactional
    public void addSeckillVoucher(Voucher voucher) {
        // 保存优惠券
        save(voucher);
        // 保存秒杀信息
        SeckillVoucher seckillVoucher = new SeckillVoucher();
        seckillVoucher.setVoucherId(voucher.getId());
        seckillVoucher.setStock(voucher.getStock());
        seckillVoucher.setBeginTime(voucher.getBeginTime());
        seckillVoucher.setEndTime(voucher.getEndTime());
        seckillVoucherService.save(seckillVoucher);
        // 添加秒杀优惠券到Redis，库存Key与预扣、回退脚本保持一致
        seckillReservation.initStock(voucher.getId(), voucher.getStock());

    }
}
