package com.hmdp.service.impl;

import com.hmdp.common.dto.Result;
import com.hmdp.common.dto.UserDTO;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.common.enums.VoucherOrderStatus;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.common.context.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class VoucherOrderPaymentTest {

    // 支付已拆到独立组件，订单查询仍由门面提供；两者共享同一份Mapper桩。
    private final VoucherOrderServiceImpl facade = new VoucherOrderServiceImpl();
    private final VoucherOrderPaymentService service = new VoucherOrderPaymentService();
    private final VoucherOrderMapper orders = mock(VoucherOrderMapper.class);

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(facade, "baseMapper", orders);
        var routes = com.hmdp.support.OrderRoutingTestSupport.install(facade);
        ReflectionTestUtils.setField(service, "voucherOrderMapper", orders);
        ReflectionTestUtils.setField(service, "voucherOrderRouterMapper", routes);
        UserDTO user = new UserDTO();
        user.setId(5L);
        UserHolder.saveUser(user);
    }

    @AfterEach
    void clearUser() {
        UserHolder.removeUser();
    }

    @Test
    void shouldPayUsingSingleConditionalUpdateWithCurrentUser() {
        when(orders.payIfUnpaid(1000L, 5L, 10L, 2)).thenReturn(1);
        Result result = service.mockPay(1000L, 2);
        assertTrue(result.getSuccess());
        assertEquals(1000L, result.getData());
        verify(orders).payIfUnpaid(1000L, 5L, 10L, 2);
        verify(orders, never()).selectRoutedOrder(anyLong(), anyLong(), anyLong());
    }

    @Test
    void shouldTreatRepeatedPaymentAsSuccessEvenAfterDeadlineWithoutChangingFirstPayment() {
        VoucherOrder paid = order(VoucherOrderStatus.PAID.getCode()).setPayType(1)
                .setPayTime(LocalDateTime.now().minusMinutes(20)).setExpireTime(LocalDateTime.now().minusMinutes(5));
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenReturn(paid);
        assertTrue(service.mockPay(1000L, 3).getSuccess());
        assertEquals(1, paid.getPayType());
        verify(orders, never()).updateById(any(VoucherOrder.class));
    }

    @Test
    void shouldRejectExpiredUnpaidOrderBasedOnDatabaseUpdateResult() {
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenReturn(order(VoucherOrderStatus.UNPAID.getCode()));
        assertEquals("订单已超时，无法支付", service.mockPay(1000L, 1).getErrorMsg());
    }

    @ParameterizedTest
    @ValueSource(ints = {3, 4, 5, 6})
    void shouldRejectOtherStatesInsteadOfOverwritingThem(int status) {
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenReturn(order(status));
        assertFalse(service.mockPay(1000L, 1).getSuccess());
        verify(orders, never()).updateById(any(VoucherOrder.class));
    }

    @Test
    void shouldNotExposeOrPayAnotherUsersOrderEvenIfItIsAlreadyPaid() {
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenReturn(order(2).setUserId(6L));
        assertEquals("订单不存在", service.mockPay(1000L, 1).getErrorMsg());
        assertEquals("订单不存在", facade.queryVoucherOrder(1000L).getErrorMsg());
        verify(orders).payIfUnpaid(1000L, 5L, 10L, 1);
    }

    @Test
    void shouldReturnOwnOrderIncludingStatusAndDeadline() {
        VoucherOrder order = order(1).setExpireTime(LocalDateTime.now().plusMinutes(15));
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenReturn(order);
        assertSame(order, facade.queryVoucherOrder(1000L).getData());
    }

    @Test
    void shouldHandleOrderNotYetCreatedWithoutTreatingItAsPaid() {
        assertEquals("订单不存在", service.mockPay(1000L, 1).getErrorMsg());
        assertFalse(facade.queryVoucherOrder(1000L).getSuccess());
    }

    @Test
    void shouldRejectUnauthenticatedCallsBeforeAccessingDatabase() {
        UserHolder.removeUser();
        assertEquals("请先登录", service.mockPay(1000L, 1).getErrorMsg());
        assertEquals("请先登录", facade.queryVoucherOrder(1000L).getErrorMsg());
        verifyNoInteractions(orders);
    }

    @Test
    void shouldRejectInvalidIdentifiersAndPaymentTypesBeforeWriting() {
        assertFalse(service.mockPay(null, 1).getSuccess());
        assertFalse(service.mockPay(0L, 1).getSuccess());
        assertFalse(service.mockPay(1000L, null).getSuccess());
        assertFalse(service.mockPay(1000L, 0).getSuccess());
        assertFalse(service.mockPay(1000L, 4).getSuccess());
        assertFalse(facade.queryVoucherOrder(-1L).getSuccess());
        verifyNoInteractions(orders);
    }

    private VoucherOrder order(int status) {
        return new VoucherOrder().setId(1000L).setUserId(5L).setVoucherId(10L).setStatus(status);
    }
}
