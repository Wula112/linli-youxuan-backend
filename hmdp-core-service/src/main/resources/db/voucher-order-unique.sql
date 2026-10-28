-- 已有数据库升级：先检查重复记录，确认结果为空且没有等价索引，再单独执行ALTER。
-- 已采用hmdp.sql新版结构或已执行voucher-order-timeout.sql的数据库不要再执行本脚本；新版使用未关闭订单唯一索引。
-- 不自动删除或合并历史订单。
SELECT user_id, voucher_id, COUNT(*) AS order_count
FROM tb_voucher_order
GROUP BY user_id, voucher_id
HAVING COUNT(*) > 1;

ALTER TABLE tb_voucher_order
    ADD UNIQUE KEY uk_order_user_voucher (user_id, voucher_id);
