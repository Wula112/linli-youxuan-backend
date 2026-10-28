package com.hmdp.service.impl;

import com.hmdp.common.dto.Result;
import com.hmdp.entity.Voucher;
import com.hmdp.cache.VoucherListCacheService;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class VoucherServiceImplTest {

    @Test
    void shouldReturnVoucherListFromCacheService() {
        VoucherListCacheService cacheService = mock(VoucherListCacheService.class);
        VoucherServiceImpl voucherService = new VoucherServiceImpl();
        ReflectionTestUtils.setField(voucherService, "voucherListCacheService", cacheService);
        List<Voucher> vouchers = List.of(new Voucher().setId(10L).setShopId(1L));
        when(cacheService.getVoucherByShopId(1L)).thenReturn(vouchers);

        Result result = voucherService.queryVoucherOfShop(1L);

        assertTrue(result.getSuccess());
        assertSame(vouchers, result.getData());
        verify(cacheService).getVoucherByShopId(1L);
    }
}
