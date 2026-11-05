-- 停止所有实例、消费者和调度；本脚本假定 hmdp_0 / hmdp_1 在同一 MySQL 实例。
-- 先备份、确认目标处理表为空；MySQL DDL 会隐式提交，执行失败需检查后从断点处理。

CREATE TABLE IF NOT EXISTS hmdp_0.tb_voucher_order_process_0 (
    order_id BIGINT NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    voucher_id BIGINT UNSIGNED NOT NULL,
    status VARCHAR(20) NOT NULL,
    PRIMARY KEY (order_id),
    KEY idx_process_status_order (status, order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS hmdp_0.tb_voucher_order_process_1 (
    order_id BIGINT NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    voucher_id BIGINT UNSIGNED NOT NULL,
    status VARCHAR(20) NOT NULL,
    PRIMARY KEY (order_id),
    KEY idx_process_status_order (status, order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS hmdp_1.tb_voucher_order_process_0 (
    order_id BIGINT NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    voucher_id BIGINT UNSIGNED NOT NULL,
    status VARCHAR(20) NOT NULL,
    PRIMARY KEY (order_id),
    KEY idx_process_status_order (status, order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS hmdp_1.tb_voucher_order_process_1 (
    order_id BIGINT NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    voucher_id BIGINT UNSIGNED NOT NULL,
    status VARCHAR(20) NOT NULL,
    PRIMARY KEY (order_id),
    KEY idx_process_status_order (status, order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO hmdp_0.tb_voucher_order_process_0 (order_id, user_id, voucher_id, status)
SELECT id, user_id, voucher_id, CASE WHEN status <> 4 THEN 'CREATED' WHEN redis_stock_restored = 1 THEN 'COMPENSATED' ELSE 'COMPENSATING' END FROM hmdp_0.tb_voucher_order_0 WHERE id % 2 = 0 AND (id DIV 2) % 2 = 0
UNION ALL
SELECT id, user_id, voucher_id, CASE WHEN status <> 4 THEN 'CREATED' WHEN redis_stock_restored = 1 THEN 'COMPENSATED' ELSE 'COMPENSATING' END FROM hmdp_0.tb_voucher_order_1 WHERE id % 2 = 0 AND (id DIV 2) % 2 = 0
UNION ALL
SELECT id, user_id, voucher_id, CASE WHEN status <> 4 THEN 'CREATED' WHEN redis_stock_restored = 1 THEN 'COMPENSATED' ELSE 'COMPENSATING' END FROM hmdp_1.tb_voucher_order_0 WHERE id % 2 = 0 AND (id DIV 2) % 2 = 0
UNION ALL
SELECT id, user_id, voucher_id, CASE WHEN status <> 4 THEN 'CREATED' WHEN redis_stock_restored = 1 THEN 'COMPENSATED' ELSE 'COMPENSATING' END FROM hmdp_1.tb_voucher_order_1 WHERE id % 2 = 0 AND (id DIV 2) % 2 = 0;

INSERT INTO hmdp_0.tb_voucher_order_process_1 (order_id, user_id, voucher_id, status)
SELECT id, user_id, voucher_id, CASE WHEN status <> 4 THEN 'CREATED' WHEN redis_stock_restored = 1 THEN 'COMPENSATED' ELSE 'COMPENSATING' END FROM hmdp_0.tb_voucher_order_0 WHERE id % 2 = 0 AND (id DIV 2) % 2 = 1
UNION ALL
SELECT id, user_id, voucher_id, CASE WHEN status <> 4 THEN 'CREATED' WHEN redis_stock_restored = 1 THEN 'COMPENSATED' ELSE 'COMPENSATING' END FROM hmdp_0.tb_voucher_order_1 WHERE id % 2 = 0 AND (id DIV 2) % 2 = 1
UNION ALL
SELECT id, user_id, voucher_id, CASE WHEN status <> 4 THEN 'CREATED' WHEN redis_stock_restored = 1 THEN 'COMPENSATED' ELSE 'COMPENSATING' END FROM hmdp_1.tb_voucher_order_0 WHERE id % 2 = 0 AND (id DIV 2) % 2 = 1
UNION ALL
SELECT id, user_id, voucher_id, CASE WHEN status <> 4 THEN 'CREATED' WHEN redis_stock_restored = 1 THEN 'COMPENSATED' ELSE 'COMPENSATING' END FROM hmdp_1.tb_voucher_order_1 WHERE id % 2 = 0 AND (id DIV 2) % 2 = 1;

INSERT INTO hmdp_1.tb_voucher_order_process_0 (order_id, user_id, voucher_id, status)
SELECT id, user_id, voucher_id, CASE WHEN status <> 4 THEN 'CREATED' WHEN redis_stock_restored = 1 THEN 'COMPENSATED' ELSE 'COMPENSATING' END FROM hmdp_0.tb_voucher_order_0 WHERE id % 2 = 1 AND (id DIV 2) % 2 = 0
UNION ALL
SELECT id, user_id, voucher_id, CASE WHEN status <> 4 THEN 'CREATED' WHEN redis_stock_restored = 1 THEN 'COMPENSATED' ELSE 'COMPENSATING' END FROM hmdp_0.tb_voucher_order_1 WHERE id % 2 = 1 AND (id DIV 2) % 2 = 0
UNION ALL
SELECT id, user_id, voucher_id, CASE WHEN status <> 4 THEN 'CREATED' WHEN redis_stock_restored = 1 THEN 'COMPENSATED' ELSE 'COMPENSATING' END FROM hmdp_1.tb_voucher_order_0 WHERE id % 2 = 1 AND (id DIV 2) % 2 = 0
UNION ALL
SELECT id, user_id, voucher_id, CASE WHEN status <> 4 THEN 'CREATED' WHEN redis_stock_restored = 1 THEN 'COMPENSATED' ELSE 'COMPENSATING' END FROM hmdp_1.tb_voucher_order_1 WHERE id % 2 = 1 AND (id DIV 2) % 2 = 0;

INSERT INTO hmdp_1.tb_voucher_order_process_1 (order_id, user_id, voucher_id, status)
SELECT id, user_id, voucher_id, CASE WHEN status <> 4 THEN 'CREATED' WHEN redis_stock_restored = 1 THEN 'COMPENSATED' ELSE 'COMPENSATING' END FROM hmdp_0.tb_voucher_order_0 WHERE id % 2 = 1 AND (id DIV 2) % 2 = 1
UNION ALL
SELECT id, user_id, voucher_id, CASE WHEN status <> 4 THEN 'CREATED' WHEN redis_stock_restored = 1 THEN 'COMPENSATED' ELSE 'COMPENSATING' END FROM hmdp_0.tb_voucher_order_1 WHERE id % 2 = 1 AND (id DIV 2) % 2 = 1
UNION ALL
SELECT id, user_id, voucher_id, CASE WHEN status <> 4 THEN 'CREATED' WHEN redis_stock_restored = 1 THEN 'COMPENSATED' ELSE 'COMPENSATING' END FROM hmdp_1.tb_voucher_order_0 WHERE id % 2 = 1 AND (id DIV 2) % 2 = 1
UNION ALL
SELECT id, user_id, voucher_id, CASE WHEN status <> 4 THEN 'CREATED' WHEN redis_stock_restored = 1 THEN 'COMPENSATED' ELSE 'COMPENSATING' END FROM hmdp_1.tb_voucher_order_1 WHERE id % 2 = 1 AND (id DIV 2) % 2 = 1;

-- 删列前逐库核对各订单表与处理表总行数、字段身份和状态；旧版已回补但未标记的记录需人工核对。

-- ALTER TABLE hmdp_0.tb_voucher_order_0 DROP INDEX idx_order_status_restore, DROP COLUMN redis_stock_restored;

-- ALTER TABLE hmdp_0.tb_voucher_order_1 DROP INDEX idx_order_status_restore, DROP COLUMN redis_stock_restored;

-- ALTER TABLE hmdp_1.tb_voucher_order_0 DROP INDEX idx_order_status_restore, DROP COLUMN redis_stock_restored;

-- ALTER TABLE hmdp_1.tb_voucher_order_1 DROP INDEX idx_order_status_restore, DROP COLUMN redis_stock_restored;
