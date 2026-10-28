package com.hmdp.framework.ratelimit;

import com.hmdp.config.WebExceptionAdvice;
import com.hmdp.controller.VoucherOrderController;
import com.hmdp.common.dto.Result;
import com.hmdp.common.dto.UserDTO;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.common.context.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class RateLimiterAspectTest {

    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final IVoucherOrderService orders = mock(IVoucherOrderService.class);
    private final MockEnvironment environment = new MockEnvironment();
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString())).thenReturn(1L);
        when(orders.secKillVoucher(anyLong())).thenReturn(Result.ok(1000L));
        VoucherOrderController controller = new VoucherOrderController();
        ReflectionTestUtils.setField(controller, "voucherOrderService", orders);
        mvc = MockMvcBuilders.standaloneSetup(proxy(controller))
                .setControllerAdvice(new WebExceptionAdvice()).build();
    }

    @AfterEach
    void clearContext() {
        UserHolder.removeUser();
        RequestContextHolder.resetRequestAttributes();
    }

    @Test
    void shouldRejectBeforeCallingSeckillBusinessAndKeepExistingFailureMessage() throws Exception {
        when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString())).thenReturn(1L, 0L);
        mvc.perform(post("/voucher-order/seckill/10"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.success").value(true));
        mvc.perform(post("/voucher-order/seckill/20"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.errorMsg").value("抢购人数过多，请稍后重试"));
        verify(orders).secKillVoucher(10L);
        verify(orders, never()).secKillVoucher(20L);
        var keys = ArgumentCaptor.forClass(List.class);
        verify(redis, times(2)).execute(any(RedisScript.class), keys.capture(), eq("1000"), eq("100"), anyString());
        assertEquals(keys.getAllValues().getFirst(), keys.getAllValues().getLast());
    }

    @Test
    void shouldResolveYamlConfigurationAndUseUniqueMembersForEachRequest() throws Exception {
        environment.withProperty("hmdp.seckill.rate-limit.window", "500ms")
                .withProperty("hmdp.seckill.rate-limit.limit", "42");
        mvc.perform(post("/voucher-order/seckill/10")).andExpect(jsonPath("$.success").value(true));
        mvc.perform(post("/voucher-order/seckill/10")).andExpect(jsonPath("$.success").value(true));
        var members = ArgumentCaptor.forClass(String.class);
        verify(redis, times(2)).execute(any(RedisScript.class), anyList(), eq("500"), eq("42"), members.capture());
        assertNotEquals(members.getAllValues().getFirst(), members.getAllValues().getLast());
    }

    @Test
    void shouldStopBusinessWhenRedisIsUnavailableOrResultIsUnknown() throws Exception {
        when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString()))
                .thenThrow(new IllegalStateException("Redis unavailable")).thenReturn((Long) null);
        mvc.perform(post("/voucher-order/seckill/10"))
                .andExpect(jsonPath("$.success").value(false));
        mvc.perform(post("/voucher-order/seckill/10"))
                .andExpect(jsonPath("$.success").value(false));
        verifyNoInteractions(orders);
    }

    @Test
    void shouldNotLimitOrderQueryOrMockPayment() throws Exception {
        when(orders.queryVoucherOrder(1000L)).thenReturn(Result.ok());
        when(orders.mockPay(1000L, 1)).thenReturn(Result.ok());
        mvc.perform(get("/voucher-order/1000")).andExpect(jsonPath("$.success").value(true));
        mvc.perform(post("/voucher-order/pay/mock/1000")).andExpect(jsonPath("$.success").value(true));
        verifyNoInteractions(redis);
    }

    @Test
    void shouldUseLoggedInUserAndIsolateDifferentUsers() {
        Endpoints endpoints = proxy(new Endpoints());
        UserDTO user = new UserDTO();
        user.setId(5L);
        UserHolder.saveUser(user);
        endpoints.byUser();
        user.setId(6L);
        endpoints.byUser();
        var keys = ArgumentCaptor.forClass(List.class);
        verify(redis, times(2)).execute(any(RedisScript.class), keys.capture(), anyString(), anyString(), anyString());
        assertTrue(keys.getAllValues().getFirst().getFirst().toString().endsWith(":user:5"));
        assertTrue(keys.getAllValues().getLast().getFirst().toString().endsWith(":user:6"));
    }

    @Test
    void shouldRejectMissingUserInsteadOfSharingAnAnonymousBucket() {
        Endpoints endpoints = proxy(new Endpoints());
        assertThrows(RateLimitException.class, endpoints::byUser);
        verifyNoInteractions(redis);
    }

    @Test
    void shouldUseRemoteAddressInsteadOfClientSuppliedForwardedHeader() {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("192.0.2.10");
        request.addHeader("X-Forwarded-For", "192.0.2.99");
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
        proxy(new Endpoints()).byIp();
        var keys = ArgumentCaptor.forClass(List.class);
        verify(redis).execute(any(RedisScript.class), keys.capture(), anyString(), anyString(), anyString());
        assertTrue(keys.getValue().getFirst().toString().endsWith(":ip:192.0.2.10"));
    }

    @Test
    void shouldKeepOverloadedMethodsInSeparateBuckets() {
        Endpoints endpoints = proxy(new Endpoints());
        endpoints.overloaded(1L);
        endpoints.overloaded("1");
        var keys = ArgumentCaptor.forClass(List.class);
        verify(redis, times(2)).execute(any(RedisScript.class), keys.capture(), anyString(), anyString(), anyString());
        assertNotEquals(keys.getAllValues().getFirst(), keys.getAllValues().getLast());
    }

    @Test
    void shouldRejectInvalidConfigurationBeforeCallingRedisOrBusiness() throws Exception {
        environment.withProperty("hmdp.seckill.rate-limit.window", "0ms");
        mvc.perform(post("/voucher-order/seckill/10")).andExpect(jsonPath("$.success").value(false));
        environment.withProperty("hmdp.seckill.rate-limit.window", "1s")
                .withProperty("hmdp.seckill.rate-limit.limit", "0");
        mvc.perform(post("/voucher-order/seckill/10")).andExpect(jsonPath("$.success").value(false));
        verifyNoInteractions(redis, orders);
    }

    private <T> T proxy(T target) {
        AspectJProxyFactory factory = new AspectJProxyFactory(target);
        factory.addAspect(new RateLimiterAspect(redis, environment));
        return factory.getProxy();
    }

    public static class Endpoints {
        @RateLimiter(type = RateLimitType.USER)
        public Result byUser() { return Result.ok(); }

        @RateLimiter(type = RateLimitType.IP)
        public Result byIp() { return Result.ok(); }

        @RateLimiter
        public Result overloaded(Long id) { return Result.ok(); }

        @RateLimiter
        public Result overloaded(String id) { return Result.ok(); }
    }
}
