-- 在已执行voucher-order-payment.sql及原一人一单唯一索引升级的数据库上执行一次。
-- 暂停订单写入后升级；保留历史订单，允许已关闭订单的用户再次抢购。
-- 先核对以下查询没有结果；原索引名称不同的数据库需按实际名称调整DROP INDEX。
SELECT user_id, voucher_id, COUNT(*) AS active_count
FROM tb_voucher_order WHERE status <> 4
GROUP BY user_id, voucher_id HAVING COUNT(*) > 1;

ALTER TABLE tb_voucher_order
    ADD COLUMN redis_stock_restored TINYINT(1) NOT NULL DEFAULT 0 COMMENT '已关闭订单的Redis库存返还是否完成',
    ADD COLUMN active_order TINYINT GENERATED ALWAYS AS (CASE WHEN status = 4 THEN NULL ELSE 1 END) STORED,
    ADD UNIQUE KEY uk_order_user_voucher_active (user_id, voucher_id, active_order),
    ADD KEY idx_order_status_expire (status, expire_time, id),
    ADD KEY idx_order_status_restore (status, redis_stock_restored, id);

-- 历史已取消订单没有本流程的返库凭据，不自动为它们重复返还库存；需单独核查历史库存。
-- 之后由closeIfExpired生成的新关闭订单会显式设置标记为0。
UPDATE tb_voucher_order
SET redis_stock_restored = 1, update_time = update_time
WHERE status = 4;
