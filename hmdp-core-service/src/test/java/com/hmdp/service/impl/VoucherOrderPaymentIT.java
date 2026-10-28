package com.hmdp.service.impl;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.MybatisSqlSessionFactoryBuilder;
import com.hmdp.common.dto.UserDTO;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.common.context.UserHolder;
import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.mapping.Environment;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.transaction.SpringManagedTransactionFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/** 仅连接专用MySQL 3308，创建随机测试库；不读取业务配置、不访问Redis或MQ。 */
@EnabledIfEnvironmentVariable(named = "HMDP_VOUCHER_PAYMENT_IT", matches = "true")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VoucherOrderPaymentIT {

    private final String schema = "hmdp_payment_it_" + UUID.randomUUID().toString().replace("-", "");
    private final JdbcTemplate server = new JdbcTemplate(new DriverManagerDataSource(
            "jdbc:mysql://127.0.0.1:3308/?allowPublicKeyRetrieval=true&useSSL=false", "root", ""));
    private final VoucherOrderServiceImpl service = new VoucherOrderServiceImpl();
    private JdbcTemplate db;
    private VoucherOrderMapper orders;

    @BeforeAll
    void start() throws Exception {
        server.execute("CREATE DATABASE " + schema);
        var dataSource = new DriverManagerDataSource("jdbc:mysql://127.0.0.1:3308/" + schema
                + "?allowPublicKeyRetrieval=true&useSSL=false", "root", "");
        db = new JdbcTemplate(dataSource);
        // 从旧版订单表开始，实际执行升级脚本，验证历史数据不被改状态或删除。
        db.execute("CREATE TABLE tb_voucher_order (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL, "
                + "voucher_id BIGINT NOT NULL, pay_type INT DEFAULT 1, status INT NOT NULL DEFAULT 1, "
                + "create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, pay_time TIMESTAMP NULL, "
                + "use_time TIMESTAMP NULL, refund_time TIMESTAMP NULL, "
                + "update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
                + "UNIQUE KEY uk_order_user_voucher(user_id,voucher_id))");
        db.update("INSERT INTO tb_voucher_order(id,user_id,voucher_id,status) VALUES (1,5,10,2),(2,6,10,4)");
        new ResourceDatabasePopulator(new ClassPathResource("db/voucher-order-payment.sql")).execute(dataSource);
        assertEquals(List.of(2, 4), db.queryForList("SELECT status FROM tb_voucher_order ORDER BY id", Integer.class));
        assertEquals(2, db.queryForObject("SELECT COUNT(*) FROM tb_voucher_order "
                + "WHERE TIMESTAMPDIFF(SECOND,create_time,expire_time)=900", Integer.class));
        new ResourceDatabasePopulator(new ClassPathResource("db/voucher-order-timeout.sql")).execute(dataSource);
        assertEquals(1, db.queryForObject("SELECT redis_stock_restored FROM tb_voucher_order WHERE id=2", Integer.class));

        MybatisConfiguration configuration = new MybatisConfiguration();
        configuration.setEnvironment(new Environment("payment-it", new SpringManagedTransactionFactory(), dataSource));
        configuration.addMapper(VoucherOrderMapper.class);
        var mapperResource = new ClassPathResource("mapper/VoucherOrderMapper.xml");
        try (var input = mapperResource.getInputStream()) {
            new XMLMapperBuilder(input, configuration, mapperResource.getPath(), configuration.getSqlFragments()).parse();
        }
        var session = new SqlSessionTemplate(new MybatisSqlSessionFactoryBuilder().build(configuration));
        orders = session.getMapper(VoucherOrderMapper.class);
        ReflectionTestUtils.setField(service, "baseMapper", orders);
        com.hmdp.support.OrderRoutingTestSupport.install(service);
    }

    @BeforeEach
    void reset() {
        db.update("DELETE FROM tb_voucher_order");
        UserDTO user = new UserDTO();
        user.setId(5L);
        UserHolder.saveUser(user);
    }

    @AfterEach
    void clearUser() {
        UserHolder.removeUser();
    }

    @AfterAll
    void stop() {
        if (!schema.matches("hmdp_payment_it_[a-f0-9]+")) throw new IllegalStateException("非法测试库名");
        server.execute("DROP DATABASE IF EXISTS " + schema);
    }

    @Test
    void shouldAllowOnlyOneStateChangeUnderConcurrentPayment() throws Exception {
        insertOrder(1, 60);
        try (ExecutorService workers = Executors.newFixedThreadPool(12)) {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Integer>> payments = IntStream.range(0, 32).mapToObj(i -> workers.submit(() -> {
                start.await();
                return orders.payIfUnpaid(1000L, 5L, 10L, i % 3 + 1);
            })).toList();
            start.countDown();
            int changed = 0;
            for (Future<Integer> payment : payments) changed += payment.get(10, TimeUnit.SECONDS);
            assertEquals(1, changed);
        }
        var paid = orders.selectRoutedOrder(1000L, 5L, 10L);
        assertEquals(2, paid.getStatus());
        assertNotNull(paid.getPayTime());
        assertTrue(service.mockPay(1000L, 3).getSuccess());
        var repeated = orders.selectRoutedOrder(1000L, 5L, 10L);
        assertEquals(paid.getPayTime(), repeated.getPayTime());
        assertEquals(paid.getPayType(), repeated.getPayType());
    }

    @Test
    void shouldRejectDeadlineReachedUsingDatabaseTime() {
        insertOrder(1, 0);
        assertFalse(service.mockPay(1000L, 1).getSuccess());
        assertEquals(1, orders.selectRoutedOrder(1000L, 5L, 10L).getStatus());
        assertNull(orders.selectRoutedOrder(1000L, 5L, 10L).getPayTime());
    }

    @Test
    void shouldRejectClosedOrderAndNotChangeItsState() {
        insertOrder(4, 60);
        assertEquals("订单已关闭，无法支付", service.mockPay(1000L, 1).getErrorMsg());
        assertEquals(4, orders.selectRoutedOrder(1000L, 5L, 10L).getStatus());
        assertNull(orders.selectRoutedOrder(1000L, 5L, 10L).getPayTime());
    }

    @Test
    void shouldNeverPayOrExposeAnotherUsersOrder() {
        insertOrder(1, 60);
        db.update("UPDATE tb_voucher_order SET user_id=6 WHERE id=1000");
        assertFalse(service.mockPay(1000L, 1).getSuccess());
        assertFalse(service.queryVoucherOrder(1000L).getSuccess());
        assertNull(orders.selectRoutedOrder(1000L, 5L, 10L));
        assertEquals(1, orders.selectRoutedOrder(1000L, 6L, 10L).getStatus());
    }

    @Test
    void shouldKeepFirstPaymentWhenRetriedAfterDeadline() {
        insertOrder(1, 60);
        assertTrue(service.mockPay(1000L, 2).getSuccess());
        LocalDateTime paidAt = orders.selectRoutedOrder(1000L, 5L, 10L).getPayTime();
        db.update("UPDATE tb_voucher_order SET expire_time=TIMESTAMPADD(SECOND,-1,NOW(3)) WHERE id=1000");
        assertTrue(service.mockPay(1000L, 3).getSuccess());
        var order = orders.selectRoutedOrder(1000L, 5L, 10L);
        assertEquals(2, order.getStatus());
        assertEquals(2, order.getPayType());
        assertEquals(paidAt, order.getPayTime());
    }

    private void insertOrder(int status, int expiresInSeconds) {
        db.update("INSERT INTO tb_voucher_order(id,user_id,voucher_id,status,expire_time) "
                + "VALUES (1000,5,10,?,TIMESTAMPADD(SECOND,?,NOW(3)))", status, expiresInSeconds);
    }
}
