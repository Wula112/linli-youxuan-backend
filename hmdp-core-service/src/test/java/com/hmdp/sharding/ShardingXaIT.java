package com.hmdp.sharding;

import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Shop;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.entity.VoucherOrderRouter;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.mapper.ShopVoucherIndexMapper;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mapper.VoucherOrderRouterMapper;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.config.GlobalConfig;
import com.baomidou.mybatisplus.core.incrementer.IdentifierGenerator;
import com.baomidou.mybatisplus.spring.MybatisSqlSessionFactoryBean;
import com.hmdp.framework.id.config.IdGeneratorConfig;
import com.hmdp.sharding.config.ShardingDataSourceConfig;
import com.hmdp.config.VoucherOrderProperties;
import com.hmdp.common.dto.UserDTO;
import com.hmdp.entity.*;
import com.hmdp.mapper.*;
import com.hmdp.mq.producer.OrderTimeoutPublisher;
import com.hmdp.mq.producer.VoucherOrderPublisher;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherService;
import com.hmdp.cache.VoucherListCacheService;
import com.hmdp.service.impl.SeckillVoucherServiceImpl;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import com.hmdp.service.impl.VoucherServiceImpl;
import com.hmdp.common.context.UserHolder;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** 仅连接本机专用 13316 端口，实际验证分片 SQL、业务事务和 XA 回滚；不连接 Redis/MQ。 */
@EnabledIfEnvironmentVariable(named = "HMDP_SHARDING_IT", matches = "true")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ShardingXaIT {
    private static final String SERVER_URL = "jdbc:mysql://127.0.0.1:13316/"
            + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai";
    private final DriverManagerDataSource serverSource = new DriverManagerDataSource(SERVER_URL, "root", "");
    private final JdbcTemplate physical = new JdbcTemplate(serverSource);
    private AnnotationConfigApplicationContext context;
    private VoucherMapper vouchers;
    private VoucherOrderMapper orders;
    private VoucherOrderRouterMapper routes;
    private IVoucherOrderService orderService;

    @BeforeAll
    void start() throws Exception {
        new ResourceDatabasePopulator(new ClassPathResource("db/sharding-schema.sql")).execute(serverSource);
        context = new AnnotationConfigApplicationContext();
        context.getEnvironment().setActiveProfiles("sharding");
        context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("sharding-it", Map.of(
                "hmdp.sharding.nodes.ds-0.url", SERVER_URL.replace("13316/", "13316/hmdp_0"),
                "hmdp.sharding.nodes.ds-1.url", SERVER_URL.replace("13316/", "13316/hmdp_1"),
                "hmdp.sharding.nodes.ds-0.username", "root",
                "hmdp.sharding.nodes.ds-1.username", "root",
                "hmdp.sharding.sql-show", "true",
                "hmdp.sharding.xa-node-id", "hmdp-sharding-it",
                "hmdp.sharding.xa-log-directory", "./target/xa-integration"
        )));
        context.register(ShardingDataSourceConfig.class, IdGeneratorConfig.class, TestConfig.class);
        context.refresh();
        vouchers = context.getBean(VoucherMapper.class);
        orders = context.getBean(VoucherOrderMapper.class);
        routes = context.getBean(VoucherOrderRouterMapper.class);
        orderService = context.getBean(IVoucherOrderService.class);
    }

    @BeforeEach
    void reset() {
        for (int db = 0; db < 2; db++) {
            for (int table = 0; table < 2; table++) {
                for (String name : List.of("tb_shop", "tb_voucher", "tb_seckill_voucher",
                        "tb_shop_voucher_index", "tb_voucher_order", "tb_voucher_order_router")) {
                    physical.execute("DELETE FROM hmdp_" + db + "." + name + "_" + table);
                }
            }
        }
        UserDTO user = new UserDTO();
        user.setId(101L);
        UserHolder.saveUser(user);
    }

    @AfterEach
    void clearUser() { UserHolder.removeUser(); }

    @AfterAll
    void stop() { if (context != null) context.close(); }

    @Test
    void shouldRouteShopsIntoAllFourPositionsAndUseSingleShopType() {
        ShopMapper shops = context.getBean(ShopMapper.class);
        for (long id = 100; id < 104; id++) {
            assertEquals(1, shops.insert(new Shop().setId(id).setName("分片店铺")
                    .setTypeId(1L).setImages("test").setArea("test").setAddress("test")
                    .setX(1D).setY(1D).setSold(0).setComments(0).setScore(0)));
            assertEquals(1, count("hmdp_" + id % 2 + ".tb_shop_" + id / 2 % 2));
            assertEquals(id, shops.selectById(id).getId());
        }
        physical.execute("DELETE FROM hmdp_0.tb_shop_type");
        physical.update("INSERT INTO hmdp_0.tb_shop_type(id,name,icon,sort) VALUES(1,'美食','test',1)");
        assertEquals("美食", context.getBean(ShopTypeMapper.class).selectById(1L).getName());
    }

    @Test
    void shouldReadShopVouchersThroughIndexAndJoinMatchingInventoryShards() {
        for (long id = 100; id < 104; id++) seedVoucher(id);
        assertEquals(List.of(100L, 101L, 102L, 103L), vouchers.queryVoucherIdsOfShop(101L));
        assertEquals(4, vouchers.queryVoucherOfShop(101L).size());
        assertTrue(vouchers.queryVoucherOfShop(999L).isEmpty());
        assertTrue(vouchers.queryVoucherOfShop(101L).stream().allMatch(voucher -> voucher.getStock() == 2));
    }

    @Test
    void shouldSelectWarmupShopsAcrossShardsUsingActivityWindowAndGlobalLimit() {
        LocalDateTime now = LocalDateTime.of(2030, 1, 1, 12, 0);
        LocalDateTime cutoff = now.plusMinutes(10);
        // 四个分片均有活动；同一商铺在不同分片有多张券，合并后只能出现一次。
        seedWarmupVoucher(100, 3, 1, 1, now.minusHours(1), now.plusHours(1));
        seedWarmupVoucher(101, 1, 1, 1, cutoff, now.plusHours(1));
        seedWarmupVoucher(102, 1, 1, 1, now, now.plusHours(1));
        seedWarmupVoucher(103, 2, 1, 1, now.plusMinutes(5), now.plusHours(1));
        // 结束边界、超出提前量、下架、普通券、非法时间均不入选。
        seedWarmupVoucher(104, 4, 1, 1, now.minusHours(1), now);
        seedWarmupVoucher(105, 5, 1, 1, cutoff.plusSeconds(1), now.plusHours(1));
        seedWarmupVoucher(106, 6, 2, 1, now, now.plusHours(1));
        seedWarmupVoucher(107, 7, 1, 0, now, now.plusHours(1));
        seedWarmupVoucher(108, 8, 1, 1, cutoff, cutoff.minusSeconds(1));
        vouchers.insert(voucher(109L).setShopId(9L)); // 没有秒杀时间记录。

        assertEquals(List.of(1L, 2L, 3L), vouchers.findWarmupShopIds(now, cutoff, 100));
        assertEquals(List.of(1L, 2L), vouchers.findWarmupShopIds(now, cutoff, 2));
        assertEquals(List.of(), vouchers.findWarmupShopIds(now.plusDays(1), cutoff.plusDays(1), 100));
    }

    private void seedWarmupVoucher(long id, long shopId, int status, int type,
                                   LocalDateTime begin, LocalDateTime end) {
        vouchers.insert(voucher(id).setShopId(shopId).setStatus(status).setType(type));
        context.getBean(SeckillVoucherMapper.class).insert(new SeckillVoucher()
                .setVoucherId(id).setStock(0).setBeginTime(begin).setEndTime(end));
    }

    @Test
    void shouldGenerateSnowflakeIdBeforeVoucherRoutingAndMaintainIndexOnMoveAndDelete() {
        IVoucherService service = context.getBean(IVoucherService.class);
        Voucher voucher = voucher(null);
        assertTrue(service.save(voucher));
        assertTrue(voucher.getId() > 9_007_199_254_740_991L);
        assertEquals(List.of(voucher.getId()), vouchers.queryVoucherIdsOfShop(101L));
        assertTrue(service.updateById(new Voucher().setId(voucher.getId()).setShopId(102L)));
        assertTrue(vouchers.queryVoucherIdsOfShop(101L).isEmpty());
        assertEquals(List.of(voucher.getId()), vouchers.queryVoucherIdsOfShop(102L));
        assertTrue(service.removeById(voucher.getId()));
        assertTrue(vouchers.queryVoucherIdsOfShop(102L).isEmpty());
    }

    @Test
    void shouldKeepOnlyTheFinalShopIndexWhenTwoTransactionsMoveTheSameVoucher() throws Exception {
        seedVoucher(100L);
        IVoucherService service = context.getBean(IVoucherService.class);
        try (var workers = java.util.concurrent.Executors.newFixedThreadPool(2)) {
            var start = new java.util.concurrent.CountDownLatch(1);
            var first = workers.submit(() -> {
                start.await();
                return service.updateById(new Voucher().setId(100L).setShopId(102L));
            });
            var second = workers.submit(() -> {
                start.await();
                return service.updateById(new Voucher().setId(100L).setShopId(103L));
            });
            start.countDown();
            assertTrue(first.get(10, java.util.concurrent.TimeUnit.SECONDS));
            assertTrue(second.get(10, java.util.concurrent.TimeUnit.SECONDS));
        }
        long finalShop = vouchers.selectById(100L).getShopId();
        for (long shopId : List.of(101L, 102L, 103L)) {
            assertEquals(shopId == finalShop ? List.of(100L) : List.of(), vouchers.queryVoucherIdsOfShop(shopId));
        }
    }

    @Test
    void shouldCommitInventoryOrderAndRouterThenPayThroughOrderId() {
        seedVoucher(100L);
        reserve(1000L);
        orderService.createVoucherOrder(order(1000L, 101L, 100L));
        assertEquals(1, stock(100L));
        assertEquals(1, count("hmdp_1.tb_voucher_order_0"));
        assertEquals(1, count("hmdp_0.tb_voucher_order_router_0"));
        assertEquals(100L, orderService.getById(1000L).getVoucherId());
        assertTrue(orderService.mockPay(1000L, 2).getSuccess());
        assertEquals(2, orders.selectRoutedOrder(1000L, 101L, 100L).getStatus());
    }

    @Test
    void shouldRollbackInventoryAndOrderWhenRouterInsertFails() {
        seedVoucher(100L);
        // 制造最终目录写入的唯一约束冲突，验证前面的跨库扣库存和订单均回滚。
        routes.insert(new VoucherOrderRouter().setOrderId(1000L).setUserId(101L).setVoucherId(100L));
        reserve(1000L);
        assertThrows(RuntimeException.class, () -> orderService.createVoucherOrder(order(1000L, 101L, 100L)));
        assertEquals(2, stock(100L));
        assertNull(orders.selectRoutedOrder(1000L, 101L, 100L));
        assertNotNull(routes.selectById(1000L));
    }

    @Test
    void shouldRollbackVoucherWhenCrossDatabaseShopIndexInsertFails() {
        context.getBean(ShopVoucherIndexMapper.class).insert(101L, 100L);
        assertThrows(RuntimeException.class, () -> context.getBean(IVoucherService.class).save(voucher(100L)));
        assertNull(vouchers.selectById(100L));
        assertEquals(List.of(100L), vouchers.queryVoucherIdsOfShop(101L));
    }

    @Test
    void shouldCloseExpiredOrderAndReturnInventoryOnlyOnce() {
        seedVoucher(100L);
        reserve(1000L);
        orderService.createVoucherOrder(order(1000L, 101L, 100L));
        physical.update("UPDATE hmdp_1.tb_voucher_order_0 SET expire_time=DATE_SUB(NOW(3),INTERVAL 1 SECOND)");
        orderService.closeExpiredOrder(1000L);
        orderService.closeExpiredOrder(1000L);
        assertEquals(4, orderService.getById(1000L).getStatus());
        assertTrue(orderService.getById(1000L).getRedisStockRestored());
        assertEquals(2, stock(100L));
        assertFalse(orderService.mockPay(1000L, 1).getSuccess());
    }

    @Test
    void shouldRollbackOrderClosureWhenInventoryReturnFails() {
        seedVoucher(100L);
        reserve(1000L);
        orderService.createVoucherOrder(order(1000L, 101L, 100L));
        physical.update("UPDATE hmdp_1.tb_voucher_order_0 SET expire_time=DATE_SUB(NOW(3),INTERVAL 1 SECOND)");
        physical.execute("DELETE FROM hmdp_0.tb_seckill_voucher_0");
        assertThrows(RuntimeException.class, () -> orderService.closeExpiredOrder(1000L));
        assertEquals(1, orderService.getById(1000L).getStatus());
    }

    @Test
    void shouldMergeScheduledOrderScanAcrossAllFourOrderShards() {
        orders.insert(order(1000L, 100L, 100L));
        orders.insert(order(1001L, 101L, 100L));
        orders.insert(order(1002L, 100L, 101L));
        orders.insert(order(1003L, 101L, 101L));
        assertEquals(List.of(1000L, 1001L), orders.findExpiredUnpaidOrders(0, 2));
        assertEquals(List.of(1002L, 1003L), orders.findExpiredUnpaidOrders(1001, 2));
    }

    @Test
    void shouldCopyOriginalDatabaseWithoutChangingIdsAndBuildBothDirectories() {
        physical.execute("CREATE DATABASE IF NOT EXISTS hmdp");
        var source = new DriverManagerDataSource(SERVER_URL.replace("13316/", "13316/hmdp"), "root", "");
        new ResourceDatabasePopulator(new ClassPathResource("db/hmdp.sql")).execute(source);
        physical.update("INSERT INTO hmdp.tb_voucher_order(id,user_id,voucher_id,status,expire_time) "
                + "VALUES(3001,101,1,1,DATE_ADD(NOW(3),INTERVAL 15 MINUTE))");
        for (String table : List.of("tb_shop_type", "tb_user", "tb_user_info", "tb_blog",
                "tb_blog_comments", "tb_follow", "tb_sign")) {
            physical.execute("DELETE FROM hmdp_0." + table);
        }
        new ResourceDatabasePopulator(new ClassPathResource("db/sharding-migrate-from-hmdp.sql"))
                .execute(serverSource);
        assertEquals(count("hmdp.tb_user"), count("hmdp_0.tb_user"));
        assertEquals(count("hmdp.tb_shop_type"), count("hmdp_0.tb_shop_type"));
        assertNotNull(orderService.getById(3001L));
        assertEquals(101L, orderService.getById(3001L).getUserId());
        assertEquals(1, physical.queryForObject(
                "SELECT active_order FROM hmdp_1.tb_voucher_order_1 WHERE id=3001", Integer.class));
        assertEquals(List.of(1L), vouchers.queryVoucherIdsOfShop(1L));
        int shops = 0;
        for (int db = 0; db < 2; db++) {
            for (int table = 0; table < 2; table++) shops += count("hmdp_" + db + ".tb_shop_" + table);
        }
        assertEquals(count("hmdp.tb_shop"), shops);
    }

    private Voucher voucher(Long id) {
        return new Voucher().setId(id).setShopId(101L).setTitle("测试券")
                .setPayValue(50L).setActualValue(100L).setType(1).setStatus(1);
    }

    private VoucherOrder order(Long id, Long userId, Long voucherId) {
        return new VoucherOrder().setId(id).setUserId(userId).setVoucherId(voucherId)
                .setStatus(1).setExpireTime(LocalDateTime.now().minusMinutes(1));
    }

    private void seedVoucher(Long id) {
        context.getBean(IVoucherService.class).save(voucher(id));
        context.getBean(SeckillVoucherMapper.class).insert(new SeckillVoucher()
                .setVoucherId(id).setStock(2).setBeginTime(LocalDateTime.now().minusHours(1))
                .setEndTime(LocalDateTime.now().plusHours(1)));
    }

    @SuppressWarnings("unchecked")
    private void reserve(Long orderId) {
        HashOperations<String, Object, Object> hashes = context.getBean(StringRedisTemplate.class).opsForHash();
        when(hashes.get(anyString(), any())).thenReturn(orderId.toString());
    }

    private int count(String table) { return physical.queryForObject("SELECT COUNT(*) FROM " + table, Integer.class); }

    private int stock(Long voucherId) {
        return context.getBean(SeckillVoucherMapper.class).selectById(voucherId).getStock();
    }

    @Configuration
    @EnableTransactionManagement
    @MapperScan("com.hmdp.mapper")
    static class TestConfig {
        @Bean
        SqlSessionFactory sqlSessionFactory(DataSource dataSource, IdentifierGenerator ids) throws Exception {
            MybatisSqlSessionFactoryBean factory = new MybatisSqlSessionFactoryBean();
            factory.setDataSource(dataSource);
            MybatisConfiguration configuration = new MybatisConfiguration();
            configuration.setMapUnderscoreToCamelCase(true);
            factory.setConfiguration(configuration);
            factory.setGlobalConfig(new GlobalConfig().setIdentifierGenerator(ids));
            factory.setMapperLocations(new PathMatchingResourcePatternResolver()
                    .getResources("classpath*:mapper/*.xml"));
            return factory.getObject();
        }

        @Bean
        PlatformTransactionManager transactionManager(DataSource dataSource) {
            return new DataSourceTransactionManager(dataSource);
        }

        @Bean IVoucherService voucherService() { return new VoucherServiceImpl(); }
        @Bean IVoucherOrderService voucherOrderService() { return new VoucherOrderServiceImpl(); }
        @Bean ISeckillVoucherService seckillVoucherService() { return new SeckillVoucherServiceImpl(); }
        @Bean VoucherOrderProperties orderProperties() { return new VoucherOrderProperties(); }
        @Bean VoucherListCacheService voucherListCacheService() { return mock(VoucherListCacheService.class); }
        @Bean com.hmdp.cache.config.VoucherCacheProperties voucherCacheProperties() {
            return new com.hmdp.cache.config.VoucherCacheProperties();
        }
        @Bean OrderTimeoutPublisher orderTimeoutPublisher() { return mock(OrderTimeoutPublisher.class); }
        @Bean VoucherOrderPublisher voucherOrderPublisher() { return mock(VoucherOrderPublisher.class); }

        @Bean
        @SuppressWarnings("unchecked")
        StringRedisTemplate stringRedisTemplate() {
            StringRedisTemplate redis = mock(StringRedisTemplate.class);
            when(redis.opsForHash()).thenReturn(mock(HashOperations.class));
            when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString()))
                    .thenReturn(1L);
            return redis;
        }

        @Bean org.redisson.api.RedissonClient redissonClient() { return mock(org.redisson.api.RedissonClient.class); }
    }
}
