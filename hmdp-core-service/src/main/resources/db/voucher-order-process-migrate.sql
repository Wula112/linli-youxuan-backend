-- 停止全部订单写入、消费者和调度后执行一次。执行前备份并确认旧返库标记准确。
-- MySQL DDL 会隐式提交；请逐步检查记录数量，不能把本脚本当作一个原子事务。
-- 单库也使用订单路由；如果旧库没有该表，按历史订单补建和回填。
CREATE TABLE IF NOT EXISTS tb_voucher_order_router (
    order_id BIGINT NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    voucher_id BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO tb_voucher_order_router (order_id, user_id, voucher_id)
SELECT id, user_id, voucher_id FROM tb_voucher_order
ON DUPLICATE KEY UPDATE order_id = tb_voucher_order_router.order_id;

CREATE TABLE IF NOT EXISTS tb_voucher_order_process (
    order_id BIGINT NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    voucher_id BIGINT UNSIGNED NOT NULL,
    status VARCHAR(20) NOT NULL,
    PRIMARY KEY (order_id),
    KEY idx_process_status_order (status, order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO tb_voucher_order_process (order_id, user_id, voucher_id, status)
SELECT id, user_id, voucher_id, CASE WHEN status <> 4 THEN 'CREATED' WHEN redis_stock_restored = 1 THEN 'COMPENSATED' ELSE 'COMPENSATING' END
FROM tb_voucher_order;

-- 在删列前核对迁移后条数、身份和状态。旧版 Redis 已返库但标记尚未写入的订单，
-- 没有新凭证时会保持 COMPENSATING，必须人工核对，不能盲目加库存。
SELECT COUNT(*) AS order_count FROM tb_voucher_order;
SELECT COUNT(*) AS process_count FROM tb_voucher_order_process;
SELECT COUNT(*) AS inconsistent_routes FROM tb_voucher_order o
LEFT JOIN tb_voucher_order_router r ON r.order_id = o.id
WHERE r.order_id IS NULL OR r.user_id <> o.user_id OR r.voucher_id <> o.voucher_id;

-- 核对完成后手动执行：
-- ALTER TABLE tb_voucher_order DROP INDEX idx_order_status_restore, DROP COLUMN redis_stock_restored;
