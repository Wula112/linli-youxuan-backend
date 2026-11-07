package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.conditions.update.UpdateChainWrapper;
import com.hmdp.common.dto.Result;
import com.hmdp.config.VoucherOrderProperties;
import com.hmdp.common.dto.UserDTO;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.entity.VoucherOrderProcess;
import com.hmdp.common.enums.OrderProcessStatus;
import com.hmdp.common.enums.VoucherOrderStatus;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mapper.VoucherOrderProcessMapper;
import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.mq.producer.VoucherOrderPublisher;
import com.hmdp.mq.producer.OrderTimeoutPublisher;
import com.hmdp.repository.redis.SeckillReservation;
import com.hmdp.service.ISeckillVoucherService;
import com.baomidou.mybatisplus.core.incrementer.IdentifierGenerator;
import com.hmdp.common.context.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.List;
import java.time.Duration;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class VoucherOrderServiceImplTest {

    private final VoucherOrderServiceImpl service = new VoucherOrderServiceImpl();
    private final VoucherOrderCreateService creator = new VoucherOrderCreateService();
    private final VoucherOrderMapper orders = mock(VoucherOrderMapper.class);
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final HashOperations<String, Object, Object> reservations = mock(HashOperations.class);
    private final ZSetOperations<String, String> pending = mock(ZSetOperations.class);
    private final VoucherOrderPublisher publisher = mock(VoucherOrderPublisher.class);
    private final IdentifierGenerator ids = mock(IdentifierGenerator.class);
    private final ISeckillVoucherService seckill = mock(ISeckillVoucherService.class);
    private final VoucherOrderProcessService process = mock(VoucherOrderProcessService.class);
    private final VoucherOrderProcessMapper processMapper = mock(VoucherOrderProcessMapper.class);
    private final SeckillCircuitBreaker circuit = mock(SeckillCircuitBreaker.class);
    private final SeckillVoucherMapper stocks = mock(SeckillVoucherMapper.class);
    private final ApplicationEventPublisher events = mock(ApplicationEventPublisher.class);
    private final List<String> clearKeys = List.of("seckill:pending", "seckill:pending:retries", "seckill:requests");
    private final VoucherOrderMessage message = new VoucherOrderMessage(1000L, 5L, 10L);

    @BeforeEach
    void setUp() {
        SeckillReservation reservation = new SeckillReservation(redis);
        ReflectionTestUtils.setField(service, "baseMapper", orders);
        var routes = com.hmdp.support.OrderRoutingTestSupport.install(service);
        ReflectionTestUtils.setField(service, "voucherOrderPublisher", publisher);
        ReflectionTestUtils.setField(service, "identifierGenerator", ids);
        ReflectionTestUtils.setField(service, "voucherOrderProcessMapper", processMapper);
        ReflectionTestUtils.setField(service, "seckillReservation", reservation);
        ReflectionTestUtils.setField(service, "circuitBreaker", circuit);
        ReflectionTestUtils.setField(creator, "processService", process);
        // 建单已拆分：测试仍从门面调用，由门面委托到真实建单组件。
        ReflectionTestUtils.setField(service, "voucherOrderCreateService", creator);
        ReflectionTestUtils.setField(creator, "voucherOrderMapper", orders);
        ReflectionTestUtils.setField(creator, "voucherOrderRouterMapper", routes);
        ReflectionTestUtils.setField(creator, "seckillReservation", reservation);
        ReflectionTestUtils.setField(creator, "seckillVoucherService", seckill);
        ReflectionTestUtils.setField(creator, "orderProperties", new VoucherOrderProperties());
        ReflectionTestUtils.setField(creator, "orderTimeoutPublisher", mock(OrderTimeoutPublisher.class));
        UserDTO user = new UserDTO();
        user.setId(5L);
        UserHolder.saveUser(user);
        when(ids.nextId(any())).thenReturn(1000L);
        when(process.lock(1000L, 5L, 10L)).thenAnswer(i -> new VoucherOrderProcess()
                .setOrderId(1000L).setUserId(5L).setVoucherId(10L).setStatus("PENDING"));
        doAnswer(i -> { VoucherOrderProcess row = i.getArgument(0);
            row.setStatus(((OrderProcessStatus) i.getArgument(1)).name()); return null; })
                .when(process).transition(any(), any());
        when(redis.opsForHash()).thenReturn(reservations);
        when(redis.opsForZSet()).thenReturn(pending);
        when(reservations.get("seckill:reservation:10", "5")).thenReturn("1000");
    }

    @AfterEach
    void tearDown() {
        UserHolder.removeUser();
        Thread.interrupted();
    }

    @Test
    void shouldReserveAndSendTransactionMessageWithoutWritingDatabaseOnRequestThread() {
        when(publisher.sendInTransaction(message)).thenReturn(SeckillReservation.SUCCESS);

        Result result = service.secKillVoucher(10L);

        assertTrue(result.getSuccess());
        assertEquals(1000L, result.getData());
        verify(publisher).sendInTransaction(message);
        verify(redis, never()).execute(any(RedisScript.class), anyList(), any(Object[].class));
        verifyNoInteractions(orders, seckill);
    }

    @Test
    void shouldNotWriteDatabaseWhenStockIsInsufficient() {
        when(publisher.sendInTransaction(message)).thenReturn(SeckillReservation.STOCK_EMPTY);

        assertFalse(service.secKillVoucher(10L).getSuccess());

        verifyNoInteractions(orders);
    }

    @Test
    void shouldReportDuplicatePurchase() {
        when(publisher.sendInTransaction(message)).thenReturn(SeckillReservation.DUPLICATE);

        assertEquals("一人只能购买一单", service.secKillVoucher(10L).getErrorMsg());

        verifyNoInteractions(orders);
    }

    @Test
    void shouldKeepReservationWhenLocalTransactionIsUnknown() {
        when(publisher.sendInTransaction(message)).thenReturn(SeckillReservation.UNKNOWN);

        assertFalse(service.secKillVoucher(10L).getSuccess());
        verifyNoInteractions(process);
        verify(redis, never()).execute(any(RedisScript.class), anyList(), any(Object[].class));
    }

    @Test
    void shouldReturnSuccessWhenUnknownTransactionHasConfirmedReservation() {
        when(publisher.sendInTransaction(message)).thenReturn(SeckillReservation.UNKNOWN);
        when(reservations.get("seckill:requests", "1000")).thenReturn("10:5:1000");

        Result result = service.secKillVoucher(10L);

        assertTrue(result.getSuccess());
        verifyNoInteractions(process);
    }

    @Test
    void shouldKeepReservationWhenUnknownResultCannotBeConfirmed() {
        when(publisher.sendInTransaction(message)).thenReturn(SeckillReservation.UNKNOWN);
        when(reservations.get("seckill:requests", "1000")).thenThrow(new IllegalStateException("Redis down"));

        Result result = service.secKillVoucher(10L);

        assertFalse(result.getSuccess());
        assertTrue(result.getErrorMsg().contains("1000"));
        verify(redis, never()).execute(any(RedisScript.class), anyList(), any(Object[].class));
    }

    @Test
    void shouldRejectNewRequestsWhenCircuitIsOpen() {
        when(circuit.isOpen()).thenReturn(true);
        assertFalse(service.secKillVoucher(10L).getSuccess());
        verifyNoInteractions(publisher);
    }

    @Test
    void shouldRejectWhenCircuitOpensAfterTheInitialCheck() {
        when(publisher.sendInTransaction(message)).thenReturn(SeckillReservation.CIRCUIT_OPEN);
        assertFalse(service.secKillVoucher(10L).getSuccess());
        verifyNoInteractions(process);
    }

    @Test
    void queryCreatesAnAcceptedOrderEvenWhileCircuitIsOpen() {
        when(circuit.isOpen()).thenReturn(true);
        when(reservations.get("seckill:requests", "1000")).thenReturn("10:5:1000");
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenReturn(null, null, order());
        databaseWritesSucceed();
        Result<VoucherOrder> result = service.queryVoucherOrder(1000L);
        assertTrue(result.getSuccess());
        assertEquals(1000L, result.getData().getId());
        verify(orders).insert(any(VoucherOrder.class));
        verifyNoInteractions(publisher);
        verify(circuit, never()).isOpen();
    }

    @Test
    void queryCannotCreateAnotherUsersAcceptedOrder() {
        when(reservations.get("seckill:requests", "1000")).thenReturn("10:6:1000");
        assertEquals("订单不存在", service.queryVoucherOrder(1000L).getErrorMsg());
        verifyNoInteractions(process);
        verify(orders, never()).insert(any(VoucherOrder.class));
    }

    @Test
    void queryRereadsOrderWhenCommitRacesWithRequestCleanup() {
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenReturn(null, order());
        assertTrue(service.queryVoucherOrder(1000L).getSuccess());
        verify(orders, never()).insert(any(VoucherOrder.class));
    }

    @Test
    void queryKeepsAcceptedRequestWhenDatabaseCreationFails() {
        when(reservations.get("seckill:requests", "1000")).thenReturn("10:5:1000");
        when(orders.selectCount(any())).thenReturn(1L);
        assertEquals("订单正在处理中，请稍后查询", service.queryVoucherOrder(1000L).getErrorMsg());
        verify(redis, never()).execute(any(RedisScript.class), eq(clearKeys), any(), any());
    }

    @Test
    void queryReportsProcessingWhenDatabaseCannotBeRead() {
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenThrow(new IllegalStateException("DB down"));
        assertEquals("订单正在处理中，请稍后查询", service.queryVoucherOrder(1000L).getErrorMsg());
        verifyNoInteractions(process);
    }

    @Test
    void shouldKeepCreateTransactionOnDedicatedComponent() throws Exception {
        var method = VoucherOrderCreateService.class.getMethod("createVoucherOrder", VoucherOrder.class);
        var transactional = method.getAnnotation(Transactional.class);
        assertNotNull(transactional);
        assertArrayEquals(new Class<?>[]{Exception.class}, transactional.rollbackFor());
        // 门面只做委托，事务注解不应放回门面，避免两处维护同一事务边界。
        assertNull(service.getClass().getMethod("createVoucherOrder", VoucherOrder.class)
                .getAnnotation(Transactional.class));
    }

    @Test
    void shouldDeductStockAndCreateOrderWithoutPublishingCacheEvent() {
        databaseWritesSucceed();
        service.createVoucherOrder(order());
        var sequence = inOrder(stocks, orders);
        sequence.verify(stocks).update(isNull(), any());
        sequence.verify(orders).insert(any(VoucherOrder.class));
        // 库存展示读 seckill:stock:*，建单不再触发券列表缓存失效。
        verifyNoInteractions(events);
    }

    @Test
    void shouldInitializeUnpaidOrderAndFixedPaymentDeadlineOnlyWhenCreating() {
        databaseWritesSucceed();
        VoucherOrder order = order().setStatus(VoucherOrderStatus.PAID.getCode()).setPayTime(LocalDateTime.now());
        service.createVoucherOrder(order);
        assertEquals(VoucherOrderStatus.UNPAID.getCode(), order.getStatus());
        assertNull(order.getPayTime());
        assertEquals(Duration.ofMinutes(15), Duration.between(order.getCreateTime(), order.getExpireTime()));
        verify(orders).insert(order);
    }

    @Test
    void shouldNotResetPaidOrderOrExtendDeadlineOnDuplicateDelivery() {
        VoucherOrder existing = order().setStatus(VoucherOrderStatus.PAID.getCode())
                .setExpireTime(LocalDateTime.now().minusMinutes(1)).setPayTime(LocalDateTime.now().minusMinutes(2));
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenReturn(existing);
        LocalDateTime deadline = existing.getExpireTime();
        service.createVoucherOrder(order());
        assertEquals(VoucherOrderStatus.PAID.getCode(), existing.getStatus());
        assertEquals(deadline, existing.getExpireTime());
        verify(orders, never()).insert(any(VoucherOrder.class));
        verifyNoInteractions(seckill);
    }

    @Test
    void shouldIgnoreRedeliveryOfCommittedOrder() {
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenReturn(order());
        service.createVoucherOrder(order());
        verifyNoInteractions(seckill, reservations, events);
        verify(redis).execute(any(RedisScript.class), eq(clearKeys), eq("10:5:1000"), eq("1000"));
        verify(orders, never()).insert(any(VoucherOrder.class));
    }

    @Test
    void shouldIgnoreLateMessageAfterCompensationDecision() {
        when(process.lock(1000L, 5L, 10L)).thenReturn(new VoucherOrderProcess()
                .setOrderId(1000L).setUserId(5L).setVoucherId(10L).setStatus("COMPENSATING"));
        when(orders.selectRoutedOrder(1000L, 5L, 10L)).thenReturn(order().setStatus(4));
        service.createVoucherOrder(order());
        verifyNoInteractions(seckill, events);
        verify(orders, never()).insert(any(VoucherOrder.class));
    }

    @Test
    void shouldRecoverLegacyUncreatedDecisionOnlyWhenStillReserved() {
        when(process.lock(1000L, 5L, 10L)).thenReturn(new VoucherOrderProcess()
                .setOrderId(1000L).setUserId(5L).setVoucherId(10L).setStatus("COMPENSATING"));
        databaseWritesSucceed();
        service.createVoucherOrder(order());
        verify(process).transition(any(), eq(OrderProcessStatus.PENDING));
        verify(process).transition(any(), eq(OrderProcessStatus.CREATED));
        verify(orders).insert(any(VoucherOrder.class));
    }

    @Test
    void shouldNotRecreateLegacyOrderWhenStockWasAlreadyReleased() {
        when(process.lock(1000L, 5L, 10L)).thenReturn(new VoucherOrderProcess()
                .setOrderId(1000L).setUserId(5L).setVoucherId(10L).setStatus("COMPENSATING"));
        when(redis.hasKey("seckill:released:1000")).thenReturn(true);
        assertThrows(IllegalStateException.class, () -> service.createVoucherOrder(order()));
        verify(orders, never()).insert(any(VoucherOrder.class));
    }

    @Test
    void shouldNotLetOldOrderUseNewOrdersReservation() {
        when(reservations.get("seckill:reservation:10", "5")).thenReturn("2000");
        assertThrows(IllegalStateException.class, () -> service.createVoucherOrder(order()));
        verifyNoInteractions(seckill, events);
    }

    @Test
    void shouldRespectExistingPurchaseUnderDifferentOrderId() {
        when(orders.selectCount(any())).thenReturn(1L);
        assertThrows(IllegalStateException.class, () -> service.createVoucherOrder(order()));
        verify(process, never()).transition(any(), eq(OrderProcessStatus.COMPENSATING));
        verifyNoInteractions(seckill, events);
    }

    @Test
    void shouldThrowOnStockFailureInsteadOfAcknowledgingIt() {
        when(orders.selectCount(any())).thenReturn(0L);
        when(seckill.update()).thenReturn(new UpdateChainWrapper<>(stocks));
        when(stocks.update(isNull(), any())).thenReturn(0);
        assertThrows(IllegalStateException.class, () -> service.createVoucherOrder(order()));
        verify(orders, never()).insert(any(VoucherOrder.class));
        verifyNoInteractions(events);
    }

    @Test
    void shouldThrowOnOrderSaveFailureSoStockTransactionRollsBack() {
        databaseWritesSucceed();
        when(orders.insert(any(VoucherOrder.class))).thenReturn(0);
        assertThrows(IllegalStateException.class, () -> service.createVoucherOrder(order()));
        verifyNoInteractions(events);
    }

    @Test
    void shouldClearPendingOnlyAfterCommitAndKeepPurchaseQualification() {
        databaseWritesSucceed();
        TransactionSynchronizationManager.initSynchronization();
        try {
            service.createVoucherOrder(order());
            verifyNoInteractions(pending);
            verify(redis, never()).execute(any(RedisScript.class), eq(clearKeys), eq("10:5:1000"), eq("1000"));
            TransactionSynchronizationManager.getSynchronizations().forEach(TransactionSynchronization::afterCommit);
            verify(redis).execute(any(RedisScript.class), eq(clearKeys), eq("10:5:1000"), eq("1000"));
            verify(reservations, never()).delete(anyString(), any());
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void shouldKeepPendingOnRollback() {
        databaseWritesSucceed();
        TransactionSynchronizationManager.initSynchronization();
        try {
            service.createVoucherOrder(order());
            TransactionSynchronizationManager.getSynchronizations()
                    .forEach(sync -> sync.afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK));
            verifyNoInteractions(pending);
            verify(redis, never()).execute(any(RedisScript.class), eq(clearKeys), eq("10:5:1000"), eq("1000"));
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void shouldLetReconciliationRetryFailedCleanupWithoutFailingCommittedOrder() {
        databaseWritesSucceed();
        when(redis.execute(any(RedisScript.class), eq(clearKeys), eq("10:5:1000"), eq("1000")))
                .thenThrow(new IllegalStateException("Redis unavailable"));
        assertDoesNotThrow(() -> service.createVoucherOrder(order()));
        verify(reservations, never()).delete(anyString(), any());
    }

    private void databaseWritesSucceed() {
        when(orders.selectCount(any())).thenReturn(0L);
        when(seckill.update()).thenReturn(new UpdateChainWrapper<>(stocks));
        when(stocks.update(isNull(), any())).thenReturn(1);
        when(orders.insert(any(VoucherOrder.class))).thenReturn(1);
    }

    private VoucherOrder order() {
        return new VoucherOrder().setId(1000L).setUserId(5L).setVoucherId(10L);
    }
}
