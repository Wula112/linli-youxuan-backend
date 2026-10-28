package com.hmdp.support;

import com.hmdp.entity.VoucherOrderRouter;
import com.hmdp.mapper.VoucherOrderRouterMapper;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class OrderRoutingTestSupport {
    private OrderRoutingTestSupport() { }

    public static VoucherOrderRouterMapper install(VoucherOrderServiceImpl service) {
        VoucherOrderRouterMapper routes = mock(VoucherOrderRouterMapper.class);
        ReflectionTestUtils.setField(service, "voucherOrderRouterMapper", routes);
        when(routes.selectById(1000L)).thenReturn(new VoucherOrderRouter()
                .setOrderId(1000L).setUserId(5L).setVoucherId(10L));
        when(routes.insert(any(VoucherOrderRouter.class))).thenReturn(1);
        return routes;
    }
}
