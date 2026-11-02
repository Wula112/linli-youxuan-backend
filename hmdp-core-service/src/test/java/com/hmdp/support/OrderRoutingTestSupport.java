package com.hmdp.support;

import com.hmdp.entity.VoucherOrderRouter;
import com.hmdp.mapper.VoucherOrderRouterMapper;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class OrderRoutingTestSupport {
    private OrderRoutingTestSupport() { }

    /** 为订单门面、支付或关单组件注入同一套路由桩。 */
    public static VoucherOrderRouterMapper install(Object service) {
        return install(service, mock(VoucherOrderRouterMapper.class));
    }

    /** 多个组件共享同一个路由桩时，用该重载保证桩只创建一次。 */
    public static VoucherOrderRouterMapper install(Object service, VoucherOrderRouterMapper routes) {
        ReflectionTestUtils.setField(service, "voucherOrderRouterMapper", routes);
        when(routes.selectById(1000L)).thenReturn(new VoucherOrderRouter()
                .setOrderId(1000L).setUserId(5L).setVoucherId(10L));
        when(routes.insert(any(VoucherOrderRouter.class))).thenReturn(1);
        return routes;
    }
}
