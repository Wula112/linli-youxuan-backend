package com.hmdp.service.impl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** 只使用本机专用测试 MySQL，随机库名避免触碰已有业务库。 */
@EnabledIfEnvironmentVariable(named = "HMDP_ORDER_PROCESS_MYSQL_IT", matches = "true")
class VoucherOrderProcessMigrationIT {
    private static final String URL = "jdbc:mysql://127.0.0.1:13316/?useSSL=false&allowPublicKeyRetrieval=true";

    @Test
    void shouldMigrateExistingOrdersBeforeRemovingLegacyColumn() {
        String name = "hmdp_process_it_" + UUID.randomUUID().toString().replace("-", "");
        var serverSource = new DriverManagerDataSource(URL, "root", "");
        JdbcTemplate server = new JdbcTemplate(serverSource);
        server.execute("CREATE DATABASE " + name);
        try {
            var source = new DriverManagerDataSource(URL.replace("13316/", "13316/" + name), "root", "");
            JdbcTemplate db = new JdbcTemplate(source);
            db.execute("CREATE TABLE tb_voucher_order (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL, "
                    + "voucher_id BIGINT NOT NULL, status INT NOT NULL, redis_stock_restored TINYINT NOT NULL, "
                    + "KEY idx_order_status_restore(status,redis_stock_restored,id))");
            db.update("INSERT INTO tb_voucher_order VALUES (1,5,10,1,0),(2,6,10,4,1),(3,7,10,4,0)");
            new ResourceDatabasePopulator(new ClassPathResource("db/voucher-order-process-migrate.sql")).execute(source);
            assertEquals("CREATED", db.queryForObject("SELECT status FROM tb_voucher_order_process WHERE order_id=1", String.class));
            assertEquals("COMPENSATED", db.queryForObject("SELECT status FROM tb_voucher_order_process WHERE order_id=2", String.class));
            assertEquals("COMPENSATING", db.queryForObject("SELECT status FROM tb_voucher_order_process WHERE order_id=3", String.class));
            assertEquals(3, db.queryForObject("SELECT COUNT(*) FROM tb_voucher_order_router r JOIN tb_voucher_order o "
                    + "ON r.order_id=o.id AND r.user_id=o.user_id AND r.voucher_id=o.voucher_id", Integer.class));
            db.execute("ALTER TABLE tb_voucher_order DROP INDEX idx_order_status_restore, DROP COLUMN redis_stock_restored");
            assertEquals(0, db.queryForObject("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=? "
                    + "AND table_name='tb_voucher_order' AND column_name='redis_stock_restored'", Integer.class, name));
        } finally {
            if (!name.matches("hmdp_process_it_[a-f0-9]{32}")) throw new IllegalStateException("非法测试库名");
            server.execute("DROP DATABASE " + name);
        }
    }

    @Test
    void shouldMigrateAllOrderShardsToOrderIdShards() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String first = "hmdp_process_a_" + suffix;
        String second = "hmdp_process_b_" + suffix;
        var source = new DriverManagerDataSource(URL, "root", "");
        JdbcTemplate db = new JdbcTemplate(source);
        db.execute("CREATE DATABASE " + first);
        db.execute("CREATE DATABASE " + second);
        try {
            String[] schemas = {first, second};
            long id = 1000;
            for (String schema : schemas) {
                for (int shard = 0; shard < 2; shard++) {
                    String table = schema + ".tb_voucher_order_" + shard;
                    db.execute("CREATE TABLE " + table + " (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL, "
                            + "voucher_id BIGINT NOT NULL, status INT NOT NULL, redis_stock_restored TINYINT NOT NULL, "
                            + "KEY idx_order_status_restore(status,redis_stock_restored,id))");
                    db.update("INSERT INTO " + table + " VALUES (?,?,?,?,?)", id++, 5, 10, 1, 0);
                }
            }
            String sql = new String(new ClassPathResource("db/voucher-order-process-sharding-migrate.sql")
                    .getInputStream().readAllBytes(), StandardCharsets.UTF_8)
                    .replace("hmdp_0", first).replace("hmdp_1", second);
            new ResourceDatabasePopulator(new ByteArrayResource(sql.getBytes(StandardCharsets.UTF_8))).execute(source);
            for (long orderId = 1000; orderId < 1004; orderId++) {
                String schema = schemas[(int) (orderId % 2)];
                String table = schema + ".tb_voucher_order_process_" + (orderId / 2 % 2);
                assertEquals("CREATED", db.queryForObject("SELECT status FROM " + table + " WHERE order_id=?",
                        String.class, orderId));
            }
        } finally {
            if (!first.matches("hmdp_process_a_[a-f0-9]{32}") || !second.matches("hmdp_process_b_[a-f0-9]{32}")) {
                throw new IllegalStateException("非法测试库名");
            }
            db.execute("DROP DATABASE " + first);
            db.execute("DROP DATABASE " + second);
        }
    }
}
