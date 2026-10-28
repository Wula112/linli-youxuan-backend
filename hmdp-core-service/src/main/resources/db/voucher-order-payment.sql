-- 已有数据库升级脚本，应用启动前执行一次；保留原有订单状态值和全部历史订单。
-- 执行期间暂停订单写入。不要通过重新导入hmdp.sql升级，原文件含DROP TABLE。
ALTER TABLE tb_voucher_order
    ADD COLUMN expire_time DATETIME(3) NULL COMMENT '支付截止时间，创建订单时确定' AFTER create_time;

-- 历史订单按默认15分钟支付窗口补齐；不会重开已支付或已取消的订单。
UPDATE tb_voucher_order
SET expire_time = DATE_ADD(create_time, INTERVAL 15 MINUTE)
WHERE expire_time IS NULL;

ALTER TABLE tb_voucher_order
    MODIFY COLUMN expire_time DATETIME(3) NOT NULL COMMENT '支付截止时间，创建订单时确定';
