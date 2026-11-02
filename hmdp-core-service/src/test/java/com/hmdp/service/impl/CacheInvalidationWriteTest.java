package com.hmdp.service.impl;

import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.ShopType;
import com.hmdp.entity.Voucher;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.mapper.ShopVoucherIndexMapper;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.repository.redis.SeckillReservation;

import com.hmdp.mq.message.CacheInvalidationMessage;
import com.hmdp.service.ISeckillVoucherService;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CacheInvalidationWriteTest {

    private final ApplicationEventPublisher events = mock(ApplicationEventPublisher.class);

    @Test
    void shouldPublishCategoryChangesForCommonCrudMethods() {
        ShopTypeMapper mapper = mock(ShopTypeMapper.class);
        ShopTypeServiceImpl service = new ShopTypeServiceImpl();
        ReflectionTestUtils.setField(service, "baseMapper", mapper);
        ReflectionTestUtils.setField(service, "eventPublisher", events);
        ShopType type = new ShopType().setId(1L).setName("美食");
        when(mapper.insert(type)).thenReturn(1);
        when(mapper.updateById(type)).thenReturn(1);
        when(mapper.deleteById((java.io.Serializable) 1L)).thenReturn(1);

        assertTrue(service.save(type));
        assertTrue(service.updateById(type));
        assertTrue(service.removeById((java.io.Serializable) 1L));

        verify(events, times(3)).publishEvent(CacheInvalidationMessage.shopType());
    }

    @Test
    void shouldNotPublishFailedWrites() {
        ShopTypeServiceImpl service = new ShopTypeServiceImpl();
        ReflectionTestUtils.setField(service, "baseMapper", mock(ShopTypeMapper.class));
        ReflectionTestUtils.setField(service, "eventPublisher", events);
        assertFalse(service.save(new ShopType()));
        assertFalse(service.updateById(new ShopType().setId(1L)));
        verifyNoInteractions(events);
    }

    @Test
    void shouldPublishOrdinaryVoucherCreationAndDeletion() {
        VoucherMapper mapper = mock(VoucherMapper.class);
        VoucherServiceImpl service = voucherService(mapper);
        Voucher voucher = new Voucher().setId(10L).setShopId(1L);
        when(mapper.insert(voucher)).thenReturn(1);
        when(mapper.selectForUpdate(10L)).thenReturn(voucher);
        when(mapper.deleteById((java.io.Serializable) 10L)).thenReturn(1);

        assertTrue(service.save(voucher));
        assertTrue(service.removeById((java.io.Serializable) 10L));

        verify(events, times(2)).publishEvent(CacheInvalidationMessage.voucherList(1L));
    }

    @Test
    void shouldInvalidateBothShopsWhenVoucherMoves() {
        VoucherMapper mapper = mock(VoucherMapper.class);
        VoucherServiceImpl service = voucherService(mapper);
        Voucher update = new Voucher().setId(10L).setShopId(2L);
        when(mapper.selectForUpdate(10L)).thenReturn(new Voucher().setId(10L).setShopId(1L));
        when(mapper.updateById(update)).thenReturn(1);

        assertTrue(service.updateById(update));

        verify(events).publishEvent(CacheInvalidationMessage.voucherList(1L));
        verify(events).publishEvent(CacheInvalidationMessage.voucherList(2L));
        verifyNoMoreInteractions(events);
    }

    @Test
    void shouldUseOriginalShopWhenPartialVoucherUpdateOmitsShopId() {
        VoucherMapper mapper = mock(VoucherMapper.class);
        VoucherServiceImpl service = voucherService(mapper);
        Voucher update = new Voucher().setId(10L).setTitle("新标题");
        when(mapper.selectForUpdate(10L)).thenReturn(new Voucher().setId(10L).setShopId(1L));
        when(mapper.updateById(update)).thenReturn(1);

        assertTrue(service.updateById(update));

        verify(events).publishEvent(CacheInvalidationMessage.voucherList(1L));
        verifyNoMoreInteractions(events);
    }

    @Test
    void shouldPublishSeckillCreationThroughVoucherSave() {
        VoucherMapper mapper = mock(VoucherMapper.class);
        VoucherServiceImpl service = voucherService(mapper);
        ISeckillVoucherService seckill = mock(ISeckillVoucherService.class);
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        ReflectionTestUtils.setField(service, "seckillVoucherService", seckill);
        ReflectionTestUtils.setField(service, "seckillReservation", new SeckillReservation(redis));
        when(redis.opsForValue()).thenReturn(values);
        when(mapper.insert(any(Voucher.class))).thenReturn(1);
        Voucher voucher = new Voucher().setId(10L).setShopId(1L).setStock(20);

        service.addSeckillVoucher(voucher);

        verify(seckill).save(any(SeckillVoucher.class));
        verify(events).publishEvent(CacheInvalidationMessage.voucherList(1L));
    }

    @Test
    void shouldPublishSeckillMetadataUpdates() {
        SeckillVoucherServiceImpl service = new SeckillVoucherServiceImpl();
        SeckillVoucherMapper mapper = mock(SeckillVoucherMapper.class);
        VoucherMapper vouchers = mock(VoucherMapper.class);
        ReflectionTestUtils.setField(service, "baseMapper", mapper);
        ReflectionTestUtils.setField(service, "voucherMapper", vouchers);
        ReflectionTestUtils.setField(service, "eventPublisher", events);
        SeckillVoucher update = new SeckillVoucher().setVoucherId(10L).setStock(30);
        when(mapper.updateById(update)).thenReturn(1);
        when(vouchers.selectById(10L)).thenReturn(new Voucher().setShopId(1L));

        assertTrue(service.updateById(update));

        verify(events).publishEvent(CacheInvalidationMessage.voucherList(1L));
    }

    private VoucherServiceImpl voucherService(VoucherMapper mapper) {
        VoucherServiceImpl service = new VoucherServiceImpl();
        ShopVoucherIndexMapper index = mock(ShopVoucherIndexMapper.class);
        when(index.insert(anyLong(), anyLong())).thenReturn(1);
        ReflectionTestUtils.setField(service, "shopVoucherIndexMapper", index);
        ReflectionTestUtils.setField(service, "baseMapper", mapper);
        ReflectionTestUtils.setField(service, "eventPublisher", events);
        return service;
    }
}
