package com.hmdp.mq;

import com.hmdp.framework.mq.config.RabbitConfig;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mapper.VoucherOrderRouterMapper;
import com.hmdp.mq.config.OrderTimeoutMqConfig;
import com.hmdp.mq.config.VoucherOrderMqConfig;
import com.hmdp.mq.config.VoucherOrderMqProperties;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.MybatisSqlSessionFactoryBuilder;
import com.hmdp.common.dto.Result;
import com.hmdp.config.VoucherOrderProperties;
import com.hmdp.common.dto.UserDTO;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.*;
import com.hmdp.mq.config.*;
import com.hmdp.mq.consumer.VoucherOrderConsumer;
import com.hmdp.mq.consumer.OrderTimeoutConsumer;
import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.mq.message.OrderTimeoutMessage;
import com.hmdp.job.OrderTimeoutJob;
import com.hmdp.mq.producer.VoucherOrderPublisher;
import com.hmdp.mq.producer.OrderTimeoutPublisher;
import com.hmdp.job.SeckillOrderReconcileJob;
import com.hmdp.job.config.SeckillReconcileProperties;
import com.hmdp.job.config.JobProperties;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.impl.SeckillVoucherServiceImpl;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import com.baomidou.mybatisplus.core.incrementer.IdentifierGenerator;
import com.hmdp.common.context.UserHolder;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.transaction.SpringManagedTransactionFactory;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.boot.amqp.autoconfigure.RabbitAutoConfiguration;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.retry.RetryException;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** 仅连接专用测试端口；创建并删除随机测试库，不读取业务 application.yaml。 */
@EnabledIfEnvironmentVariable(named = "HMDP_VOUCHER_ORDER_IT", matches = "true")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VoucherOrderFlowIT {

    private final String schema = "hmdp_voucher_it_" + UUID.randomUUID().toString().replace("-", "");
    private final JdbcTemplate server = new JdbcTemplate(new DriverManagerDataSource(
            "jdbc:mysql://127.0.0.1:3308/?allowPublicKeyRetrieval=true&useSSL=false", "root", ""));
    private final long voucherId = 900010L;
    private final List<String> keys = List.of("seckill:stock:" + voucherId,
            "seckill:reservation:" + voucherId, "seckill:pending", "seckill:pending:retries");
    private final AtomicLong ids = new AtomicLong(1000L);
    private JdbcTemplate db;
    private LettuceConnectionFactory redisConnection;
    private StringRedisTemplate redis;
    private RedissonClient redisson;
    private AnnotationConfigApplicationContext context;
    private VoucherOrderServiceImpl target;
    private IVoucherOrderService service;
    private VoucherOrderPublisher publisher;
    private OrderTimeoutPublisher timeoutPublisher;
    private VoucherOrderMapper orderMapper;
    private RabbitTemplate rabbit;
    private RabbitAdmin admin;

    @BeforeAll
    void start() throws Exception {
        server.execute("CREATE DATABASE " + schema);
        var dataSource = new DriverManagerDataSource("jdbc:mysql://127.0.0.1:3308/" + schema
                + "?allowPublicKeyRetrieval=true&useSSL=false", "root", "");
        db = new JdbcTemplate(dataSource);
        db.execute("CREATE TABLE tb_seckill_voucher (voucher_id BIGINT PRIMARY KEY, stock INT NOT NULL, "
                + "begin_time DATETIME, end_time DATETIME, create_time DATETIME, update_time DATETIME)");
        db.execute("CREATE TABLE tb_voucher_order (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL, "
                + "voucher_id BIGINT NOT NULL, pay_type INT DEFAULT 1 CHECK(pay_type BETWEEN 1 AND 3), "
                + "status INT DEFAULT 1, create_time DATETIME DEFAULT CURRENT_TIMESTAMP, "
                + "expire_time DATETIME(3) NOT NULL, redis_stock_restored TINYINT NOT NULL DEFAULT 0, "
                + "active_order TINYINT GENERATED ALWAYS AS (CASE WHEN status=4 THEN NULL ELSE 1 END) STORED, pay_time DATETIME, "
                + "use_time DATETIME, refund_time DATETIME, update_time DATETIME, "
                + "UNIQUE KEY uk_order_user_voucher_active(user_id, voucher_id, active_order))");
        MybatisConfiguration configuration = new MybatisConfiguration();
        configuration.setEnvironment(new Environment("it", new SpringManagedTransactionFactory(), dataSource));
        configuration.addMapper(VoucherOrderMapper.class);
        configuration.addMapper(VoucherOrderRouterMapper.class);
        db.execute("CREATE TABLE tb_voucher_order_router(order_id BIGINT PRIMARY KEY, "
                + "user_id BIGINT NOT NULL, voucher_id BIGINT NOT NULL)");
        configuration.addMapper(SeckillVoucherMapper.class);
        var orderResource = new ClassPathResource("mapper/VoucherOrderMapper.xml");
        try (var input = orderResource.getInputStream()) {
            new XMLMapperBuilder(input, configuration, orderResource.getPath(), configuration.getSqlFragments()).parse();
        }
        var session = new SqlSessionTemplate(new MybatisSqlSessionFactoryBuilder().build(configuration));
        var seckill = new SeckillVoucherServiceImpl();
        ReflectionTestUtils.setField(seckill, "baseMapper", session.getMapper(SeckillVoucherMapper.class));
        redisConnection = new LettuceConnectionFactory("127.0.0.1", 6388);
        redisConnection.afterPropertiesSet();
        redisConnection.start();
        redis = new StringRedisTemplate(redisConnection);
        Config redissonConfig = new Config();
        redissonConfig.useSingleServer().setAddress("redis://127.0.0.1:6388");
        redisson = Redisson.create(redissonConfig);
        target = spy(new VoucherOrderServiceImpl());
        orderMapper = session.getMapper(VoucherOrderMapper.class);
        ReflectionTestUtils.setField(target, "baseMapper", orderMapper);
        ReflectionTestUtils.setField(target, "voucherOrderRouterMapper", session.getMapper(VoucherOrderRouterMapper.class));
        ReflectionTestUtils.setField(target, "seckillVoucherService", seckill);
        ReflectionTestUtils.setField(target, "stringRedisTemplate", redis);
        ReflectionTestUtils.setField(target, "redissonClient", redisson);
        ReflectionTestUtils.setField(target, "eventPublisher", mock(ApplicationEventPublisher.class));
        ReflectionTestUtils.setField(target, "orderProperties", new VoucherOrderProperties());
        ReflectionTestUtils.setField(target, "orderTimeoutPublisher", mock(OrderTimeoutPublisher.class));
        VoucherMapper vouchers = mock(VoucherMapper.class);
        when(vouchers.selectById(voucherId)).thenReturn(new Voucher().setId(voucherId).setShopId(1L));
        ReflectionTestUtils.setField(target, "voucherMapper", vouchers);
        IdentifierGenerator idWorker = mock(IdentifierGenerator.class);
        when(idWorker.nextId(any())).thenAnswer(invocation -> ids.incrementAndGet());
        ReflectionTestUtils.setField(target, "identifierGenerator", idWorker);
        ProxyFactory proxy = new ProxyFactory(target);
        proxy.setInterfaces(IVoucherOrderService.class);
        var transactionManager = new DataSourceTransactionManager(dataSource);
        ReflectionTestUtils.setField(target, "transactionManager", transactionManager);
        proxy.addAdvice(new TransactionInterceptor(transactionManager,
                new AnnotationTransactionAttributeSource()));
        service = (IVoucherOrderService) proxy.getProxy();

        context = new AnnotationConfigApplicationContext();
        context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("test", Map.ofEntries(
                Map.entry("spring.rabbitmq.port", "5678"),
                Map.entry("spring.rabbitmq.publisher-confirm-type", "correlated"),
                Map.entry("spring.rabbitmq.publisher-returns", "true"),
                Map.entry("spring.rabbitmq.listener.simple.acknowledge-mode", "auto"),
                Map.entry("spring.rabbitmq.listener.simple.concurrency", "2"),
                Map.entry("spring.rabbitmq.listener.simple.default-requeue-rejected", "false"),
                Map.entry("spring.rabbitmq.listener.simple.retry.enabled", "true"),
                Map.entry("spring.rabbitmq.listener.simple.retry.max-retries", "2"),
                Map.entry("spring.rabbitmq.listener.simple.retry.initial-interval", "20ms"))));
        context.register(RabbitAutoConfiguration.class, RabbitConfig.class, VoucherOrderMqConfig.class);
        context.registerBean(VoucherOrderMqProperties.class, () -> {
            var properties = new VoucherOrderMqProperties();
            properties.setRetryInterval(Duration.ofMillis(10));
            properties.setConfirmTimeout(Duration.ofSeconds(2));
            return properties;
        });
        context.registerBean(IVoucherOrderService.class, () -> service);
        context.registerBean(RedissonClient.class, () -> redisson);
        context.registerBean(VoucherOrderProperties.class, () -> {
            var properties = new VoucherOrderProperties();
            properties.setPayTimeout(Duration.ofSeconds(1));
            return properties;
        });
        context.register(VoucherOrderConsumer.class, VoucherOrderPublisher.class,
                OrderTimeoutMqConfig.class, OrderTimeoutPublisher.class, OrderTimeoutConsumer.class);
        context.refresh();
        publisher = context.getBean(VoucherOrderPublisher.class);
        timeoutPublisher = context.getBean(OrderTimeoutPublisher.class);
        rabbit = context.getBean(RabbitTemplate.class);
        admin = context.getBean(RabbitAdmin.class);
        ReflectionTestUtils.setField(target, "voucherOrderPublisher", publisher);
    }

    @BeforeEach
    void reset() {
        admin.purgeQueue(VoucherOrderMqConfig.QUEUE);
        admin.purgeQueue(VoucherOrderMqConfig.DEAD_QUEUE);
        admin.purgeQueue(OrderTimeoutMqConfig.DELAY_QUEUE);
        admin.purgeQueue(OrderTimeoutMqConfig.CLOSE_QUEUE);
        admin.purgeQueue(OrderTimeoutMqConfig.DEAD_QUEUE);
        db.update("DELETE FROM tb_voucher_order");
        db.update("DELETE FROM tb_voucher_order_router");
        db.update("DELETE FROM tb_seckill_voucher");
        db.update("INSERT INTO tb_seckill_voucher(voucher_id,stock) VALUES (?,2)", voucherId);
        redis.delete(keys);
        redis.opsForValue().set(keys.getFirst(), "2");
        UserDTO user = new UserDTO();
        user.setId(5L);
        UserHolder.saveUser(user);
        clearInvocations(target);
        ReflectionTestUtils.setField(target, "baseMapper", orderMapper);
        ReflectionTestUtils.setField(target, "stringRedisTemplate", redis);
        ReflectionTestUtils.setField(target, "orderTimeoutPublisher", mock(OrderTimeoutPublisher.class));
        ReflectionTestUtils.setField(target, "orderProperties", new VoucherOrderProperties());
    }

    @AfterEach
    void clearUser() {
        UserHolder.removeUser();
    }

    @AfterAll
    void stop() {
        if (context != null && context.isActive()) {
            context.getBean(RabbitListenerEndpointRegistry.class).stop();
            for (String queue : List.of(OrderTimeoutMqConfig.DELAY_QUEUE, OrderTimeoutMqConfig.CLOSE_QUEUE,
                    OrderTimeoutMqConfig.DEAD_QUEUE)) admin.deleteQueue(queue);
            for (String exchange : List.of(OrderTimeoutMqConfig.DELAY_EXCHANGE, OrderTimeoutMqConfig.CLOSE_EXCHANGE,
                    OrderTimeoutMqConfig.DEAD_EXCHANGE)) admin.deleteExchange(exchange);
            context.close();
        }
        if (redisson != null && !redisson.isShutdown()) redisson.shutdown();
        if (redis != null) redis.delete(keys);
        if (redisConnection != null) redisConnection.destroy();
        if (!schema.matches("hmdp_voucher_it_[a-f0-9]+")) throw new IllegalStateException("非法测试库名");
        server.execute("DROP DATABASE IF EXISTS " + schema);
    }

    @Test
    void shouldCreateOrderThroughRabbitAndHandleDuplicateDelivery() throws Exception {
        Result result = service.secKillVoucher(voucherId);
        assertTrue(result.getSuccess());
        Long orderId = (Long) result.getData();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertEquals(1, orderCount()));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertNull(redis.opsForZSet().score(keys.get(2), member(5L, orderId))));
        assertEquals(orderId.toString(), redis.opsForHash().get(keys.get(1), "5"));
        publisher.publish(new VoucherOrderMessage(orderId, 5L, voucherId));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> verify(target, times(2)).createVoucherOrder(any()));
        assertEquals(1, stock());
        assertEquals("1", redis.opsForValue().get(keys.getFirst()));
        assertEquals(1, orderCount());
    }

    @Test
    void shouldRollbackDatabaseStockWhenInsertFails() {
        assertEquals(0L, reserve(5L, 7001L));
        VoucherOrder invalid = new VoucherOrder().setId(7001L).setUserId(5L).setVoucherId(voucherId).setPayType(99);
        assertThrows(RuntimeException.class, () -> service.createVoucherOrder(invalid));
        assertEquals(2, stock());
        assertEquals(0, orderCount());
        assertNotNull(redis.opsForZSet().score(keys.get(2), member(5L, 7001L)));
    }

    @Test
    void shouldRetryDatabaseFailureThenRouteToDeadLetterQueue() {
        db.update("UPDATE tb_seckill_voucher SET stock=0");
        assertTrue(service.secKillVoucher(voucherId).getSuccess());
        Message[] dead = new Message[1];
        await().atMost(Duration.ofSeconds(10)).until(() -> {
            dead[0] = rabbit.receive(VoucherOrderMqConfig.DEAD_QUEUE);
            return dead[0] != null;
        });
        assertNotNull(dead[0].getMessageProperties().getHeaders().get("x-death"));
        verify(target, times(3)).createVoucherOrder(any());
        assertEquals(0, stock());
        assertEquals(0, orderCount());
        assertEquals(1L, redis.opsForZSet().zCard(keys.get(2)));
    }

    @Test
    void shouldIgnoreLateMessageAfterPublishFailureAndAllowNewRequest() throws Exception {
        VoucherOrderPublisher failedPublisher = mock(VoucherOrderPublisher.class);
        doThrow(new RetryException("confirm lost", new TimeoutException())).when(failedPublisher).publish(any());
        ReflectionTestUtils.setField(target, "voucherOrderPublisher", failedPublisher);
        long oldId;
        try {
            assertFalse(service.secKillVoucher(voucherId).getSuccess());
            oldId = ids.get();
        } finally {
            ReflectionTestUtils.setField(target, "voucherOrderPublisher", publisher);
        }
        assertEquals("2", redis.opsForValue().get(keys.getFirst()));
        assertNull(redis.opsForZSet().score(keys.get(2), member(5L, oldId)));
        Result retry = service.secKillVoucher(voucherId);
        assertTrue(retry.getSuccess());
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertEquals(1, orderCount()));
        publisher.publish(new VoucherOrderMessage(oldId, 5L, voucherId));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> verify(target, times(2)).createVoucherOrder(any()));
        assertEquals(1, stock());
        assertEquals("1", redis.opsForValue().get(keys.getFirst()));
        assertEquals(1, orderCount());
        assertEquals(retry.getData(), db.queryForObject("SELECT id FROM tb_voucher_order", Long.class));
    }

    @Test
    void shouldAtomicallyReserveAndOnlyRollbackMatchingOrderOnce() throws Exception {
        redis.opsForValue().set(keys.getFirst(), "5");
        try (ExecutorService workers = Executors.newFixedThreadPool(10)) {
            List<Future<Long>> results = IntStream.rangeClosed(1, 20)
                    .mapToObj(i -> workers.submit(() -> reserve((long) i, 8000L + i))).toList();
            int successes = 0;
            for (Future<Long> result : results) if (result.get(5, TimeUnit.SECONDS) == 0) successes++;
            assertEquals(5, successes);
        }
        assertEquals("0", redis.opsForValue().get(keys.getFirst()));
        var reservation = redis.opsForHash().entries(keys.get(1)).entrySet().iterator().next();
        String user = reservation.getKey().toString();
        String order = reservation.getValue().toString();
        // 库存为0时，已有资格的用户优先得到重复购买结果。
        assertEquals(2L, reserve(Long.parseLong(user), 9999L));
        String oldMember = voucherId + ":" + user + ":" + order;
        assertEquals(1L, redis.execute(script("seckill-rollback.lua"), keys, user, order, oldMember));
        assertNull(redis.opsForZSet().score(keys.get(2), oldMember));
        assertEquals(0L, redis.execute(script("seckill-rollback.lua"), keys, user, order, oldMember));
        assertEquals(0L, reserve(Long.parseLong(user), 9000L));
        assertEquals(0L, redis.execute(script("seckill-rollback.lua"), keys, user, order, oldMember));
        assertEquals("0", redis.opsForValue().get(keys.getFirst()));
        assertEquals("9000", redis.opsForHash().get(keys.get(1), user));
        assertNotNull(redis.opsForZSet().score(keys.get(2), member(Long.valueOf(user), 9000L)));
        assertFalse(Boolean.TRUE.equals(redis.hasKey("seckill:order:" + voucherId)));
    }

    @Test
    void shouldRecoverReservationLeftByProducerCrash() {
        assertEquals(0L, reserve(5L, 7101L));
        expirePending(5L, 7101L);
        // 预扣之后从未发送消息，模拟Producer在两步之间退出。
        reconciler(publisher).reconcile();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertEquals(1, orderCount());
            assertNull(redis.opsForZSet().score(keys.get(2), member(5L, 7101L)));
        });
        assertEquals(7101L, db.queryForObject("SELECT id FROM tb_voucher_order", Long.class));
        assertEquals(1, stock());
        assertEquals("7101", redis.opsForHash().get(keys.get(1), "5"));
    }

    @Test
    void shouldClearResidualPendingForCommittedOrderWithoutReleasingQualification() {
        assertEquals(0L, reserve(5L, 7102L));
        service.createVoucherOrder(new VoucherOrder().setId(7102L).setUserId(5L).setVoucherId(voucherId));
        // 构造“数据库已提交、pending清理遗漏”的恢复场景。
        expirePending(5L, 7102L);
        reconciler(publisher).reconcile();
        assertNull(redis.opsForZSet().score(keys.get(2), member(5L, 7102L)));
        assertEquals("7102", redis.opsForHash().get(keys.get(1), "5"));
        assertEquals(2L, reserve(5L, 9102L));
        assertEquals(1, stock());
    }

    @Test
    void shouldClaimOnceAcrossInstancesAndNeverReinsertRemovedPending() throws Exception {
        assertEquals(0L, reserve(5L, 7103L));
        expirePending(5L, 7103L);
        String member = member(5L, 7103L);
        long deadline = System.currentTimeMillis() - Duration.ofMinutes(5).toMillis();
        try (ExecutorService workers = Executors.newFixedThreadPool(8)) {
            List<Future<Long>> claims = IntStream.range(0, 16).mapToObj(i -> workers.submit(() ->
                    redis.execute(script("seckill-pending-claim.lua"), List.of(keys.get(2)), member,
                            Long.toString(deadline), Long.toString(System.currentTimeMillis())))).toList();
            int winners = 0;
            for (Future<Long> claim : claims) if (claim.get(5, TimeUnit.SECONDS) == 1L) winners++;
            assertEquals(1, winners);
        }
        redis.opsForZSet().remove(keys.get(2), member);
        assertEquals(0L, redis.execute(script("seckill-pending-claim.lua"), List.of(keys.get(2)), member,
                Long.toString(deadline), Long.toString(System.currentTimeMillis())));
        assertNull(redis.opsForZSet().score(keys.get(2), member));
        assertEquals("7103", redis.opsForHash().get(keys.get(1), "5"));
    }

    @Test
    void shouldKeepReservationAndDeferAfterReconcilePublishFailure() throws Exception {
        assertEquals(0L, reserve(5L, 7104L));
        expirePending(5L, 7104L);
        VoucherOrderPublisher failed = mock(VoucherOrderPublisher.class);
        doThrow(new RetryException("MQ unavailable", new TimeoutException())).when(failed).publish(any());
        SeckillOrderReconcileJob job = reconciler(failed);
        job.reconcile();
        job.reconcile();
        verify(failed).publish(new VoucherOrderMessage(7104L, 5L, voucherId, 1));
        assertNotNull(redis.opsForZSet().score(keys.get(2), member(5L, 7104L)));
        assertEquals("7104", redis.opsForHash().get(keys.get(1), "5"));
        assertEquals("1", redis.opsForValue().get(keys.getFirst()));
        assertEquals(2, stock());
        assertEquals(0, orderCount());
    }

    @Test
    void shouldReleaseAfterFinalRepublishGraceAndIgnoreLateRabbitMessageAfterUserRetries() throws Exception {
        long oldId = 7110L;
        assertEquals(0L, reserve(5L, oldId));
        VoucherOrderPublisher failed = mock(VoucherOrderPublisher.class);
        doThrow(new RetryException("MQ unavailable", new TimeoutException())).when(failed).publish(any());
        SeckillReconcileProperties properties = new SeckillReconcileProperties();
        properties.setMaxRepublishRetries(1);
        SeckillOrderReconcileJob job = new SeckillOrderReconcileJob(redis, orderMapper, failed, properties, redisson, new JobProperties());

        expirePending(5L, oldId);
        job.reconcile();
        job.reconcile(); // 最后一轮补发后尚未经过等待窗口，不能立即释放。
        verify(failed).publish(new VoucherOrderMessage(oldId, 5L, voucherId, 1));
        assertEquals("1", redis.opsForValue().get(keys.getFirst()));
        assertEquals(Long.toString(oldId), redis.opsForHash().get(keys.get(1), "5"));
        assertEquals("1", redis.opsForHash().get(keys.get(3), member(5L, oldId)));

        expirePending(5L, oldId);
        job.reconcile();
        job.reconcile(); // 重复扫描不能再次返库。
        assertEquals(2, stock());
        assertEquals(0, orderCount());
        assertEquals("2", redis.opsForValue().get(keys.getFirst()));
        assertNull(redis.opsForHash().get(keys.get(1), "5"));
        assertNull(redis.opsForZSet().score(keys.get(2), member(5L, oldId)));
        assertNull(redis.opsForHash().get(keys.get(3), member(5L, oldId)));

        Result retried = service.secKillVoucher(voucherId);
        assertTrue(retried.getSuccess());
        awaitOrder((Long) retried.getData());
        publisher.publish(new VoucherOrderMessage(oldId, 5L, voucherId));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> verify(target, times(2)).createVoucherOrder(any()));
        assertEquals(1, orderCount());
        assertEquals(1, stock());
        assertEquals("1", redis.opsForValue().get(keys.getFirst()));
        assertEquals(retried.getData().toString(), redis.opsForHash().get(keys.get(1), "5"));
    }

    @Test
    void shouldKeepExhaustedReservationWhileConsumerTransactionHasNotCommitted() throws Exception {
        long id = 7111L;
        prepareExhaustedReservation(id);
        CountDownLatch transactionReady = new CountDownLatch(1);
        CountDownLatch allowCommit = new CountDownLatch(1);
        doAnswer(invocation -> {
            Object result = invocation.callRealMethod();
            transactionReady.countDown();
            if (!allowCommit.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("等待提交超时");
            return result;
        }).when(target).createVoucherOrder(any());
        try (ExecutorService worker = Executors.newSingleThreadExecutor()) {
            Future<?> consuming = worker.submit(() -> context.getBean(VoucherOrderConsumer.class)
                    .consume(new VoucherOrderMessage(id, 5L, voucherId)));
            try {
                assertTrue(transactionReady.await(5, TimeUnit.SECONDS));
                reconciler(publisher).reconcile();
                assertEquals(0, orderCount()); // 独立连接仍看不到未提交事务。
                assertEquals("1", redis.opsForValue().get(keys.getFirst()));
                assertEquals(Long.toString(id), redis.opsForHash().get(keys.get(1), "5"));
                assertNotNull(redis.opsForZSet().score(keys.get(2), member(5L, id)));
                assertEquals("5", redis.opsForHash().get(keys.get(3), member(5L, id)));
            } finally {
                allowCommit.countDown();
            }
            consuming.get(10, TimeUnit.SECONDS);
        } finally {
            doCallRealMethod().when(target).createVoucherOrder(any());
        }
        assertEquals(1, orderCount());
        assertEquals(1, stock());
        assertEquals("1", redis.opsForValue().get(keys.getFirst()));
        assertEquals(Long.toString(id), redis.opsForHash().get(keys.get(1), "5"));
        assertNull(redis.opsForZSet().score(keys.get(2), member(5L, id)));
    }

    @Test
    void shouldReadCommittedOrderAgainWhenConsumerFinishesBetweenInitialQueryAndLock() {
        long id = 7112L;
        prepareExhaustedReservation(id);
        RedissonClient intervening = mock(RedissonClient.class);
        when(intervening.getLock("lock:order:" + voucherId + ":5")).thenAnswer(invocation -> {
            context.getBean(VoucherOrderConsumer.class).consume(new VoucherOrderMessage(id, 5L, voucherId));
            return redisson.getLock(invocation.getArgument(0, String.class));
        });
        new SeckillOrderReconcileJob(redis, orderMapper, publisher, new SeckillReconcileProperties(), intervening, new JobProperties())
                .reconcile();
        verify(intervening).getLock("lock:order:" + voucherId + ":5");
        assertEquals(1, orderCount());
        assertEquals(1, stock());
        assertEquals("1", redis.opsForValue().get(keys.getFirst()));
        assertEquals(Long.toString(id), redis.opsForHash().get(keys.get(1), "5"));
        assertNull(redis.opsForZSet().score(keys.get(2), member(5L, id)));
    }

    @Test
    void shouldRetainPendingWhenReleaseFailsAndCompleteOnNextCheck() {
        long id = 7113L;
        prepareExhaustedReservation(id);
        redis.delete(keys.getFirst());
        SeckillOrderReconcileJob job = reconciler(publisher);
        job.reconcile();
        assertNotNull(redis.opsForZSet().score(keys.get(2), member(5L, id)));
        assertEquals("5", redis.opsForHash().get(keys.get(3), member(5L, id)));
        assertEquals(Long.toString(id), redis.opsForHash().get(keys.get(1), "5"));
        // 仅在隔离测试中恢复已知库存，验证前次失败没有提前清理资格或补发预算。
        redis.opsForValue().set(keys.getFirst(), "1");
        expirePending(5L, id);
        job.reconcile();
        assertEquals(0, orderCount());
        assertEquals(2, stock());
        assertEquals("2", redis.opsForValue().get(keys.getFirst()));
        assertNull(redis.opsForHash().get(keys.get(1), "5"));
        assertNull(redis.opsForZSet().score(keys.get(2), member(5L, id)));
        assertNull(redis.opsForHash().get(keys.get(3), member(5L, id)));
    }

    private void prepareExhaustedReservation(long orderId) {
        assertEquals(0L, reserve(5L, orderId));
        expirePending(5L, orderId);
        redis.opsForHash().put(keys.get(3), member(5L, orderId), "5");
    }

    @Test
    void shouldCloseThroughRealTtlDeadLetterAndAllowSameUserToBuyAgain() throws Exception {
        ReflectionTestUtils.setField(target, "orderTimeoutPublisher", timeoutPublisher);
        ReflectionTestUtils.setField(target, "orderProperties", context.getBean(VoucherOrderProperties.class));
        Long oldId = (Long) service.secKillVoucher(voucherId).getData();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            VoucherOrder order = orderMapper.selectById(oldId);
            assertNotNull(order);
            assertEquals(4, order.getStatus());
            assertTrue(order.getRedisStockRestored());
        });
        assertEquals(2, stock());
        assertEquals("2", redis.opsForValue().get(keys.getFirst()));
        assertNull(redis.opsForHash().get(keys.get(1), "5"));

        // 新单使用正常支付窗口，保留已关闭订单历史；旧通知重复到达不能影响新预占。
        ReflectionTestUtils.setField(target, "orderTimeoutPublisher", mock(OrderTimeoutPublisher.class));
        ReflectionTestUtils.setField(target, "orderProperties", new VoucherOrderProperties());
        Result retry = service.secKillVoucher(voucherId);
        assertTrue(retry.getSuccess());
        Long newId = (Long) retry.getData();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertEquals(2, orderCount()));
        timeoutPublisher.publish(new OrderTimeoutMessage(oldId));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> verify(target, atLeast(2)).closeExpiredOrder(oldId));
        assertEquals(1, stock());
        assertEquals("1", redis.opsForValue().get(keys.getFirst()));
        assertEquals(newId.toString(), redis.opsForHash().get(keys.get(1), "5"));
        assertEquals(1, orderMapper.selectById(newId).getStatus());
    }

    @Test
    void shouldKeepPaidOrderWhenDelayedCloseNotificationArrives() {
        ReflectionTestUtils.setField(target, "orderTimeoutPublisher", timeoutPublisher);
        Result result = service.secKillVoucher(voucherId);
        Long id = (Long) result.getData();
        awaitOrder(id);
        assertTrue(service.mockPay(id, 2).getSuccess());
        expireOrder(id);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> verify(target).closeExpiredOrder(id));
        assertEquals(2, orderMapper.selectById(id).getStatus());
        assertEquals(1, stock());
        assertEquals("1", redis.opsForValue().get(keys.getFirst()));
        assertEquals(id.toString(), redis.opsForHash().get(keys.get(1), "5"));
    }

    @Test
    void shouldCloseFromDatabaseScanWhenDelayNotificationWasNotSent() {
        Long id = (Long) service.secKillVoucher(voucherId).getData();
        awaitOrder(id);
        expireOrder(id);
        timeoutJob().closeExpiredOrders();
        assertEquals(4, orderMapper.selectById(id).getStatus());
        assertTrue(orderMapper.selectById(id).getRedisStockRestored());
        assertEquals(2, stock());
        assertEquals("2", redis.opsForValue().get(keys.getFirst()));
    }

    @Test
    void shouldRecoverRedisFailureAfterCloseCommitWithoutDoubleDatabaseRestore() {
        Long id = (Long) service.secKillVoucher(voucherId).getData();
        awaitOrder(id);
        expireOrder(id);
        StringRedisTemplate unavailable = mock(StringRedisTemplate.class);
        when(unavailable.execute(any(DefaultRedisScript.class), anyList(), anyString(), anyString(), anyString()))
                .thenThrow(new IllegalStateException("Redis unavailable"));
        ReflectionTestUtils.setField(target, "stringRedisTemplate", unavailable);
        try {
            assertThrows(IllegalStateException.class, () -> service.closeExpiredOrder(id));
        } finally {
            ReflectionTestUtils.setField(target, "stringRedisTemplate", redis);
        }
        assertEquals(4, orderMapper.selectById(id).getStatus());
        assertFalse(orderMapper.selectById(id).getRedisStockRestored());
        assertEquals(2, stock());
        assertEquals("1", redis.opsForValue().get(keys.getFirst()));
        timeoutJob().closeExpiredOrders();
        service.closeExpiredOrder(id);
        assertEquals(2, stock());
        assertEquals("2", redis.opsForValue().get(keys.getFirst()));
        assertTrue(orderMapper.selectById(id).getRedisStockRestored());
    }

    @Test
    void shouldNotTouchNewReservationWhenOldRedisRestoreMarkerWasNotCommitted() {
        Long oldId = (Long) service.secKillVoucher(voucherId).getData();
        awaitOrder(oldId);
        expireOrder(oldId);
        VoucherOrderMapper failing = spy(orderMapper);
        doThrow(new IllegalStateException("marker unavailable")).when(failing)
                .markRedisStockRestored(oldId, 5L, voucherId);
        ReflectionTestUtils.setField(target, "baseMapper", failing);
        try {
            assertThrows(IllegalStateException.class, () -> service.closeExpiredOrder(oldId));
        } finally {
            ReflectionTestUtils.setField(target, "baseMapper", orderMapper);
        }
        assertEquals(2, stock());
        assertEquals("2", redis.opsForValue().get(keys.getFirst()));
        assertFalse(orderMapper.selectById(oldId).getRedisStockRestored());
        Result retry = service.secKillVoucher(voucherId);
        assertTrue(retry.getSuccess());
        Long newId = (Long) retry.getData();
        awaitOrder(newId);
        service.closeExpiredOrder(oldId);
        assertTrue(orderMapper.selectById(oldId).getRedisStockRestored());
        assertEquals(1, stock());
        assertEquals("1", redis.opsForValue().get(keys.getFirst()));
        assertEquals(newId.toString(), redis.opsForHash().get(keys.get(1), "5"));
    }

    @Test
    void shouldRestoreOnlyOnceForConcurrentCloseRequests() throws Exception {
        Long id = (Long) service.secKillVoucher(voucherId).getData();
        awaitOrder(id);
        expireOrder(id);
        try (ExecutorService workers = Executors.newFixedThreadPool(8)) {
            var closes = IntStream.range(0, 16)
                    .mapToObj(i -> workers.submit(() -> service.closeExpiredOrder(id))).toList();
            for (Future<?> close : closes) close.get(10, TimeUnit.SECONDS);
        }
        assertEquals(4, orderMapper.selectById(id).getStatus());
        assertEquals(2, stock());
        assertEquals("2", redis.opsForValue().get(keys.getFirst()));
    }

    @Test
    void shouldRollbackCloseOnDatabaseStockFailureThenDeadLetterAndRecover() throws Exception {
        Long id = (Long) service.secKillVoucher(voucherId).getData();
        awaitOrder(id);
        expireOrder(id);
        db.update("DELETE FROM tb_seckill_voucher");
        timeoutPublisher.publish(new OrderTimeoutMessage(id));
        await().atMost(Duration.ofSeconds(10)).until(() -> rabbit.receive(OrderTimeoutMqConfig.DEAD_QUEUE) != null);
        assertEquals(1, orderMapper.selectById(id).getStatus());
        assertEquals(id.toString(), redis.opsForHash().get(keys.get(1), "5"));
        db.update("INSERT INTO tb_seckill_voucher(voucher_id,stock) VALUES (?,1)", voucherId);
        timeoutJob().closeExpiredOrders();
        assertEquals(4, orderMapper.selectById(id).getStatus());
        assertEquals(2, stock());
        assertEquals("2", redis.opsForValue().get(keys.getFirst()));
    }

    @Test
    void shouldKeepSingleOutcomeWhenPaymentAndCloseRace() throws Exception {
        Long id = (Long) service.secKillVoucher(voucherId).getData();
        awaitOrder(id);
        db.update("UPDATE tb_voucher_order SET expire_time=TIMESTAMPADD(MICROSECOND,10000,NOW(3)) WHERE id=?", id);
        try (ExecutorService workers = Executors.newFixedThreadPool(8)) {
            var actions = IntStream.range(0, 16).mapToObj(i -> workers.submit(() -> {
                if (i % 2 == 0) {
                    UserDTO user = new UserDTO();
                    user.setId(5L);
                    UserHolder.saveUser(user);
                    try { service.mockPay(id, 1); } finally { UserHolder.removeUser(); }
                } else {
                    service.closeExpiredOrder(id);
                }
            })).toList();
            for (Future<?> action : actions) action.get(10, TimeUnit.SECONDS);
        }
        expireOrder(id);
        service.closeExpiredOrder(id);
        int status = orderMapper.selectById(id).getStatus();
        assertTrue(status == 2 || status == 4);
        assertEquals(status == 2 ? 1 : 2, stock());
        assertEquals(status == 2 ? "1" : "2", redis.opsForValue().get(keys.getFirst()));
    }

    private void awaitOrder(Long id) {
        await().atMost(Duration.ofSeconds(5)).until(() -> orderMapper.selectById(id) != null);
    }

    private void expireOrder(Long id) {
        db.update("UPDATE tb_voucher_order SET expire_time=TIMESTAMPADD(SECOND,-1,NOW(3)) WHERE id=?", id);
    }

    private OrderTimeoutJob timeoutJob() {
        return new OrderTimeoutJob(orderMapper, service, new VoucherOrderProperties(), redis, redisson, new JobProperties());
    }

    private SeckillOrderReconcileJob reconciler(VoucherOrderPublisher sender) {
        return new SeckillOrderReconcileJob(redis, target.getBaseMapper(), sender, new SeckillReconcileProperties(), redisson, new JobProperties());
    }

    private String member(Long user, Long order) {
        return new VoucherOrderMessage(order, user, voucherId).pendingMember();
    }

    private void expirePending(Long user, Long order) {
        redis.opsForZSet().add(keys.get(2), member(user, order),
                System.currentTimeMillis() - Duration.ofMinutes(6).toMillis());
    }

    private Long reserve(Long user, Long order) {
        return redis.execute(script("seckill.lua"), keys, user.toString(), order.toString(),
                Long.toString(voucherId), Long.toString(System.currentTimeMillis()));
    }

    private DefaultRedisScript<Long> script(String name) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource(name));
        script.setResultType(Long.class);
        return script;
    }

    private int orderCount() { return db.queryForObject("SELECT COUNT(*) FROM tb_voucher_order", Integer.class); }
    private int stock() { return db.queryForObject("SELECT stock FROM tb_seckill_voucher", Integer.class); }
}
