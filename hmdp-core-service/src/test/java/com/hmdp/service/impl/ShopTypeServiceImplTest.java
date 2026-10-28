package com.hmdp.service.impl;

import com.hmdp.common.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.cache.ShopTypeCacheService;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ShopTypeServiceImplTest {

    @Test
    void shouldReturnSortedTypesFromCacheService() {
        ShopTypeCacheService cacheService = mock(ShopTypeCacheService.class);
        ShopTypeServiceImpl shopTypeService = new ShopTypeServiceImpl();
        ReflectionTestUtils.setField(shopTypeService, "shopTypeCacheService", cacheService);
        List<ShopType> types = List.of(new ShopType().setId(1L).setName("美食").setSort(1));
        when(cacheService.getTypeList()).thenReturn(types);

        Result result = shopTypeService.queryTypeList();

        assertTrue(result.getSuccess());
        assertSame(types, result.getData());
        verify(cacheService).getTypeList();
    }
}
