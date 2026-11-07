package com.hmdp.mq.consumer;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.spring.MybatisSqlSessionFactoryBean;
import com.hmdp.common.enums.OrderProcessStatus;
import com.hmdp.common.context.UserHolder;
import com.hmdp.common.dto.UserDTO;
import com.hmdp.entity.VoucherOrderProcess;
import com.hmdp.job.SeckillOrderReconcileJob;
import com.hmdp.job.config.JobProperties;
import com.hmdp.job.config.SeckillReconcileProperties;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mapper.VoucherOrderProcessMapper;
import com.hmdp.mapper.VoucherOrderRouterMapper;
import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.mq.support.MqMessageCodec;
import com.hmdp.repository.redis.SeckillPendingRepository;
import com.hmdp.repository.redis.SeckillReservation;
import com.hmdp.config.VoucherOrderProperties;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.impl.SeckillVoucherServiceImpl;
import com.hmdp.service.impl.SeckillCircuitBreaker;
import com.hmdp.service.impl.VoucherOrderCreateService;
import com.hmdp.service.impl.VoucherOrderCloseService;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import com.hmdp.mq.producer.OrderTimeoutPublisher;
import com.hmdp.service.impl.VoucherOrderCompensationService;
import com.hmdp.service.impl.VoucherOrderProcessService;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.mybatis.spring.annotation.MapperScan;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.test.util.ReflectionTestUtils;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.hmdp.common.constant.RedisConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/** 仅使用隔离 MySQL 13316、Redis 16389 和 RocketMQ 19876/20911。 */
@EnabledIfEnvironmentVariable(named = "HMDP_ORDER_RECOVERY_E2E", matches = "true")
class VoucherOrderRecoveryIT {
    @Test
    void deadLetterOpensCircuitAndAcceptedOrderIsCreatedBeforeClosedStockIsReleased() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String schema = "hmdp_recovery_it_" + suffix;
        String address = "jdbc:mysql://127.0.0.1:13316/";
        JdbcTemplate server = new JdbcTemplate(new DriverManagerDataSource(
                address + "?useSSL=false&allowPublicKeyRetrieval=true", "root", ""));
        server.execute("CREATE DATABASE " + schema);
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        RedissonClient redisson = null;
        DefaultMQProducer producer = null;
        DefaultMQPushConsumer normal = null;
        DefaultMQPushConsumer dead = null;
        long orderId = ThreadLocalRandom.current().nextLong(1_000_000_000_000L, 9_000_000_000_000L);
        long voucherId = ThreadLocalRandom.current().nextLong(1_000_000_000_000L, 9_000_000_000_000L);
        long userId = 5L;
        try {
            DataSource source = new DriverManagerDataSource(address + schema
                    + "?useSSL=false&allowPublicKeyRetrieval=true", "root", "");
            JdbcTemplate db = new JdbcTemplate(source);
            db.execute("CREATE TABLE tb_voucher_order (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL, "
                    + "voucher_id BIGINT NOT NULL, status INT NOT NULL, pay_type INT DEFAULT 1, "
                    + "create_time DATETIME(3), expire_time DATETIME(3), pay_time DATETIME(3), "
                    + "use_time DATETIME(3), refund_time DATETIME(3), update_time DATETIME(3))");
            db.execute("CREATE TABLE tb_voucher_order_process (order_id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL, "
                    + "voucher_id BIGINT NOT NULL, status VARCHAR(20) NOT NULL, KEY idx_state(status, order_id))");
            db.execute("CREATE TABLE tb_voucher_order_router (order_id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL, voucher_id BIGINT NOT NULL)");
            db.execute("CREATE TABLE tb_seckill_voucher (voucher_id BIGINT PRIMARY KEY, stock INT NOT NULL)");
            db.update("INSERT INTO tb_seckill_voucher(voucher_id, stock) VALUES (?, 1)", voucherId);
            context.registerBean(DataSource.class, () -> source);
            context.register(TestConfiguration.class);
            context.refresh();
            StringRedisTemplate redis = context.getBean(StringRedisTemplate.class);
            SeckillReservation reservation = context.getBean(SeckillReservation.class);
            redis.opsForValue().set(SECKILL_STOCK_KEY + voucherId, "1");
            assertEquals(0L, reservation.reserve(voucherId, userId, orderId));

            Config redisConfig = new Config();
            redisConfig.useSingleServer().setAddress("redis://127.0.0.1:16389");
            redisson = Redisson.create(redisConfig);
            IVoucherOrderService failingOrders = mock(IVoucherOrderService.class);
            doThrow(new IllegalStateException("test failure")).when(failingOrders).createVoucherOrder(any());
            String group = "hmdp-recovery-it-" + suffix.substring(0, 12);
            String topic = "hmdp-recovery-topic-" + suffix.substring(0, 12);
            producer = new DefaultMQProducer("hmdp-recovery-p-" + suffix.substring(0, 12));
            producer.setNamesrvAddr("127.0.0.1:19876");
            producer.start();
            VoucherOrderMessage event = new VoucherOrderMessage(orderId, userId, voucherId);
            producer.send(new Message(topic, "create", MqMessageCodec.encode(event)));

            dead = new DefaultMQPushConsumer("hmdp-recovery-d-" + suffix.substring(0, 12));
            dead.setNamesrvAddr("127.0.0.1:19876");
            dead.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            dead.subscribe("%DLQ%" + group, "*");
            dead.registerMessageListener(new VoucherOrderDeadLetterConsumer(context.getBean(VoucherOrderProcessService.class),
                    context.getBean(SeckillCircuitBreaker.class)));
            dead.start();
            normal = new DefaultMQPushConsumer(group);
            normal.setNamesrvAddr("127.0.0.1:19876");
            normal.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            normal.setMaxReconsumeTimes(0);
            normal.subscribe(topic, "*");
            normal.registerMessageListener(new VoucherOrderConsumer(failingOrders, redisson));
            normal.start();

            long deadline = System.nanoTime() + Duration.ofSeconds(55).toNanos();
            VoucherOrderProcessMapper mapper = context.getBean(VoucherOrderProcessMapper.class);
            VoucherOrderProcess row = null;
            while (System.nanoTime() < deadline) {
                row = mapper.selectById(orderId);
                if (row != null && OrderProcessStatus.DEAD_LETTER.name().equals(row.getStatus())) break;
                Thread.sleep(250);
            }
            assertNotNull(row, "死信未写入处理记录");
            assertEquals("DEAD_LETTER", row.getStatus());
            assertTrue(context.getBean(SeckillCircuitBreaker.class).isOpen());
            assertEquals(SeckillReservation.CIRCUIT_OPEN, reservation.reserve(voucherId, userId + 1, orderId + 1));
            SeckillReconcileProperties properties = new SeckillReconcileProperties();
            properties.setPendingTimeout(Duration.ofMillis(1));
            SeckillOrderReconcileJob job = new SeckillOrderReconcileJob(new SeckillPendingRepository(redis),
                    context.getBean(VoucherOrderMapper.class), mapper, context.getBean(VoucherOrderCreateService.class),
                    context.getBean(VoucherOrderCompensationService.class), context.getBean(SeckillCircuitBreaker.class), reservation, properties,
                    new JobProperties(), redis, redisson);
            assertFalse(job.reconcileBatch().successful(), "熔断期间必须向XXL上报失败以触发告警");
            assertEquals("CREATED", mapper.selectById(orderId).getStatus());
            assertEquals(1, db.queryForObject("SELECT COUNT(*) FROM tb_voucher_order WHERE id=?", Integer.class, orderId));
            assertEquals("0", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
            assertEquals(Long.toString(orderId), reservation.getReservedOrderId(voucherId, userId));
            job.reconcileBatch();
            assertEquals(0, db.queryForObject("SELECT stock FROM tb_seckill_voucher WHERE voucher_id=?", Integer.class, voucherId));

            db.update("UPDATE tb_voucher_order SET expire_time=DATE_SUB(NOW(3),INTERVAL 1 SECOND) WHERE id=?", orderId);
            context.getBean(VoucherOrderCloseService.class).closeExpiredOrder(orderId);
            assertEquals("COMPENSATING", mapper.selectById(orderId).getStatus());
            job.reconcileBatch();
            assertEquals("COMPENSATED", mapper.selectById(orderId).getStatus());
            assertEquals("1", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));
            assertEquals(1, db.queryForObject("SELECT stock FROM tb_seckill_voucher WHERE voucher_id=?", Integer.class, voucherId));
            job.reconcileBatch();
            assertEquals("1", redis.opsForValue().get(SECKILL_STOCK_KEY + voucherId));

            // 熔断人工恢复后，用户可在消息尚未消费时直接触发真实数据库建单。
            redis.delete(SECKILL_CIRCUIT_KEY);
            db.update("INSERT INTO tb_seckill_voucher(voucher_id, stock) VALUES (?, 1)", voucherId + 1);
            redis.opsForValue().set(SECKILL_STOCK_KEY + (voucherId + 1), "1");
            assertEquals(0L, reservation.reserve(voucherId + 1, userId, orderId + 1));
            VoucherOrderServiceImpl facade = new VoucherOrderServiceImpl();
            ReflectionTestUtils.setField(facade, "baseMapper", context.getBean(VoucherOrderMapper.class));
            ReflectionTestUtils.setField(facade, "voucherOrderRouterMapper", context.getBean(VoucherOrderRouterMapper.class));
            ReflectionTestUtils.setField(facade, "voucherOrderProcessMapper", mapper);
            ReflectionTestUtils.setField(facade, "voucherOrderCreateService", context.getBean(VoucherOrderCreateService.class));
            ReflectionTestUtils.setField(facade, "seckillReservation", reservation);
            UserDTO user = new UserDTO();
            user.setId(userId);
            UserHolder.saveUser(user);
            try {
                assertTrue(facade.queryVoucherOrder(orderId + 1).getSuccess());
            } finally {
                UserHolder.removeUser();
            }
            assertEquals("CREATED", mapper.selectById(orderId + 1).getStatus());
            assertNull(reservation.findAcceptedRequestMember(orderId + 1));
            assertEquals(0, db.queryForObject("SELECT stock FROM tb_seckill_voucher WHERE voucher_id=?", Integer.class, voucherId + 1));
        } finally {
            if (normal != null) normal.shutdown();
            if (dead != null) dead.shutdown();
            if (producer != null) producer.shutdown();
            if (redisson != null) redisson.shutdown();
            if (context.isActive()) {
                StringRedisTemplate redis = context.getBean(StringRedisTemplate.class);
                redis.delete(List.of(SECKILL_STOCK_KEY + voucherId, SECKILL_RESERVATION_KEY + voucherId,
                        SECKILL_STOCK_KEY + (voucherId + 1), SECKILL_RESERVATION_KEY + (voucherId + 1),
                        SECKILL_TXN_KEY + orderId, SECKILL_TXN_KEY + (orderId + 1),
                        SECKILL_RELEASED_KEY + orderId, SECKILL_CIRCUIT_KEY));
                redis.opsForZSet().remove(SECKILL_PENDING_KEY, voucherId + ":" + userId + ":" + orderId,
                        (voucherId + 1) + ":" + userId + ":" + (orderId + 1));
                redis.opsForHash().delete(SECKILL_REQUESTS_KEY, Long.toString(orderId), Long.toString(orderId + 1));
                context.close();
            }
            if (!schema.matches("hmdp_recovery_it_[a-f0-9]{32}")) throw new IllegalStateException("非法测试库名");
            server.execute("DROP DATABASE " + schema);
        }
    }

    @Configuration
    @EnableTransactionManagement
    @MapperScan("com.hmdp.mapper")
    static class TestConfiguration {
        @Bean SqlSessionFactory sqlSessionFactory(DataSource source) throws Exception {
            MybatisSqlSessionFactoryBean factory = new MybatisSqlSessionFactoryBean();
            factory.setDataSource(source);
            MybatisConfiguration configuration = new MybatisConfiguration();
            configuration.setMapUnderscoreToCamelCase(true);
            factory.setConfiguration(configuration);
            factory.setMapperLocations(new PathMatchingResourcePatternResolver().getResources("classpath*:mapper/*.xml"));
            return factory.getObject();
        }
        @Bean PlatformTransactionManager transactionManager(DataSource source) { return new DataSourceTransactionManager(source); }
        @Bean LettuceConnectionFactory redisConnectionFactory() { return new LettuceConnectionFactory("127.0.0.1", 16389); }
        @Bean StringRedisTemplate stringRedisTemplate(LettuceConnectionFactory connection) { return new StringRedisTemplate(connection); }
        @Bean SeckillReservation reservation(StringRedisTemplate redis) { return new SeckillReservation(redis); }
        @Bean VoucherOrderProcessService processes(VoucherOrderProcessMapper mapper, VoucherOrderMapper orders) {
            return new VoucherOrderProcessService(mapper, orders);
        }
        @Bean VoucherOrderCompensationService compensation(VoucherOrderProcessMapper mapper,
                VoucherOrderProcessService processes, SeckillReservation reservation, VoucherOrderMapper orders) {
            return new VoucherOrderCompensationService(mapper, processes, reservation, orders);
        }
        @Bean SeckillCircuitBreaker circuit(StringRedisTemplate redis) { return new SeckillCircuitBreaker(redis); }
        @Bean VoucherOrderCreateService creator() { return new VoucherOrderCreateService(); }
        @Bean VoucherOrderCloseService closer() { return new VoucherOrderCloseService(); }
        @Bean ISeckillVoucherService stockService() { return new SeckillVoucherServiceImpl(); }
        @Bean VoucherOrderProperties orderProperties() { return new VoucherOrderProperties(); }
        @Bean OrderTimeoutPublisher orderTimeoutPublisher() { return mock(OrderTimeoutPublisher.class); }
    }
}
