-- 先执行 sharding-schema.sql，并停止应用写入。源 hmdp 必须已升级到当前订单表结构。
-- 仅用于同一 MySQL 实例内从 hmdp 复制到空分片，保留所有旧 ID。重复执行将因主键冲突失败。
-- 如中途报错，请在该连接 ROLLBACK；全部成功才 COMMIT。

START TRANSACTION;

INSERT INTO hmdp_0.tb_blog (`id`, `shop_id`, `user_id`, `title`, `images`, `content`, `liked`, `comments`, `create_time`, `update_time`) SELECT `id`, `shop_id`, `user_id`, `title`, `images`, `content`, `liked`, `comments`, `create_time`, `update_time` FROM hmdp.tb_blog;

INSERT INTO hmdp_0.tb_blog_comments (`id`, `user_id`, `blog_id`, `parent_id`, `answer_id`, `content`, `liked`, `status`, `create_time`, `update_time`) SELECT `id`, `user_id`, `blog_id`, `parent_id`, `answer_id`, `content`, `liked`, `status`, `create_time`, `update_time` FROM hmdp.tb_blog_comments;

INSERT INTO hmdp_0.tb_follow (`id`, `user_id`, `follow_user_id`, `create_time`) SELECT `id`, `user_id`, `follow_user_id`, `create_time` FROM hmdp.tb_follow;

INSERT INTO hmdp_0.tb_shop_type (`id`, `name`, `icon`, `sort`, `create_time`, `update_time`) SELECT `id`, `name`, `icon`, `sort`, `create_time`, `update_time` FROM hmdp.tb_shop_type;

INSERT INTO hmdp_0.tb_sign (`id`, `user_id`, `year`, `month`, `date`, `is_backup`) SELECT `id`, `user_id`, `year`, `month`, `date`, `is_backup` FROM hmdp.tb_sign;

INSERT INTO hmdp_0.tb_user (`id`, `phone`, `password`, `nick_name`, `icon`, `create_time`, `update_time`) SELECT `id`, `phone`, `password`, `nick_name`, `icon`, `create_time`, `update_time` FROM hmdp.tb_user;

INSERT INTO hmdp_0.tb_user_info (`user_id`, `city`, `introduce`, `fans`, `followee`, `gender`, `birthday`, `credits`, `level`, `create_time`, `update_time`) SELECT `user_id`, `city`, `introduce`, `fans`, `followee`, `gender`, `birthday`, `credits`, `level`, `create_time`, `update_time` FROM hmdp.tb_user_info;

INSERT INTO hmdp_0.tb_shop_0 (`id`, `name`, `type_id`, `images`, `area`, `address`, `x`, `y`, `avg_price`, `sold`, `comments`, `score`, `open_hours`, `create_time`, `update_time`)
SELECT `id`, `name`, `type_id`, `images`, `area`, `address`, `x`, `y`, `avg_price`, `sold`, `comments`, `score`, `open_hours`, `create_time`, `update_time` FROM hmdp.tb_shop WHERE id % 2 = 0 AND (id DIV 2) % 2 = 0;

INSERT INTO hmdp_0.tb_shop_1 (`id`, `name`, `type_id`, `images`, `area`, `address`, `x`, `y`, `avg_price`, `sold`, `comments`, `score`, `open_hours`, `create_time`, `update_time`)
SELECT `id`, `name`, `type_id`, `images`, `area`, `address`, `x`, `y`, `avg_price`, `sold`, `comments`, `score`, `open_hours`, `create_time`, `update_time` FROM hmdp.tb_shop WHERE id % 2 = 0 AND (id DIV 2) % 2 = 1;

INSERT INTO hmdp_1.tb_shop_0 (`id`, `name`, `type_id`, `images`, `area`, `address`, `x`, `y`, `avg_price`, `sold`, `comments`, `score`, `open_hours`, `create_time`, `update_time`)
SELECT `id`, `name`, `type_id`, `images`, `area`, `address`, `x`, `y`, `avg_price`, `sold`, `comments`, `score`, `open_hours`, `create_time`, `update_time` FROM hmdp.tb_shop WHERE id % 2 = 1 AND (id DIV 2) % 2 = 0;

INSERT INTO hmdp_1.tb_shop_1 (`id`, `name`, `type_id`, `images`, `area`, `address`, `x`, `y`, `avg_price`, `sold`, `comments`, `score`, `open_hours`, `create_time`, `update_time`)
SELECT `id`, `name`, `type_id`, `images`, `area`, `address`, `x`, `y`, `avg_price`, `sold`, `comments`, `score`, `open_hours`, `create_time`, `update_time` FROM hmdp.tb_shop WHERE id % 2 = 1 AND (id DIV 2) % 2 = 1;

INSERT INTO hmdp_0.tb_voucher_0 (`id`, `shop_id`, `title`, `sub_title`, `rules`, `pay_value`, `actual_value`, `type`, `status`, `create_time`, `update_time`)
SELECT `id`, `shop_id`, `title`, `sub_title`, `rules`, `pay_value`, `actual_value`, `type`, `status`, `create_time`, `update_time` FROM hmdp.tb_voucher WHERE id % 2 = 0 AND (id DIV 2) % 2 = 0;

INSERT INTO hmdp_0.tb_voucher_1 (`id`, `shop_id`, `title`, `sub_title`, `rules`, `pay_value`, `actual_value`, `type`, `status`, `create_time`, `update_time`)
SELECT `id`, `shop_id`, `title`, `sub_title`, `rules`, `pay_value`, `actual_value`, `type`, `status`, `create_time`, `update_time` FROM hmdp.tb_voucher WHERE id % 2 = 0 AND (id DIV 2) % 2 = 1;

INSERT INTO hmdp_1.tb_voucher_0 (`id`, `shop_id`, `title`, `sub_title`, `rules`, `pay_value`, `actual_value`, `type`, `status`, `create_time`, `update_time`)
SELECT `id`, `shop_id`, `title`, `sub_title`, `rules`, `pay_value`, `actual_value`, `type`, `status`, `create_time`, `update_time` FROM hmdp.tb_voucher WHERE id % 2 = 1 AND (id DIV 2) % 2 = 0;

INSERT INTO hmdp_1.tb_voucher_1 (`id`, `shop_id`, `title`, `sub_title`, `rules`, `pay_value`, `actual_value`, `type`, `status`, `create_time`, `update_time`)
SELECT `id`, `shop_id`, `title`, `sub_title`, `rules`, `pay_value`, `actual_value`, `type`, `status`, `create_time`, `update_time` FROM hmdp.tb_voucher WHERE id % 2 = 1 AND (id DIV 2) % 2 = 1;

INSERT INTO hmdp_0.tb_seckill_voucher_0 (`voucher_id`, `stock`, `create_time`, `begin_time`, `end_time`, `update_time`)
SELECT `voucher_id`, `stock`, `create_time`, `begin_time`, `end_time`, `update_time` FROM hmdp.tb_seckill_voucher WHERE voucher_id % 2 = 0 AND (voucher_id DIV 2) % 2 = 0;

INSERT INTO hmdp_0.tb_seckill_voucher_1 (`voucher_id`, `stock`, `create_time`, `begin_time`, `end_time`, `update_time`)
SELECT `voucher_id`, `stock`, `create_time`, `begin_time`, `end_time`, `update_time` FROM hmdp.tb_seckill_voucher WHERE voucher_id % 2 = 0 AND (voucher_id DIV 2) % 2 = 1;

INSERT INTO hmdp_1.tb_seckill_voucher_0 (`voucher_id`, `stock`, `create_time`, `begin_time`, `end_time`, `update_time`)
SELECT `voucher_id`, `stock`, `create_time`, `begin_time`, `end_time`, `update_time` FROM hmdp.tb_seckill_voucher WHERE voucher_id % 2 = 1 AND (voucher_id DIV 2) % 2 = 0;

INSERT INTO hmdp_1.tb_seckill_voucher_1 (`voucher_id`, `stock`, `create_time`, `begin_time`, `end_time`, `update_time`)
SELECT `voucher_id`, `stock`, `create_time`, `begin_time`, `end_time`, `update_time` FROM hmdp.tb_seckill_voucher WHERE voucher_id % 2 = 1 AND (voucher_id DIV 2) % 2 = 1;

INSERT INTO hmdp_0.tb_voucher_order_0 (`id`, `user_id`, `voucher_id`, `pay_type`, `status`, `create_time`, `expire_time`, `pay_time`, `use_time`, `refund_time`, `update_time`)
SELECT `id`, `user_id`, `voucher_id`, `pay_type`, `status`, `create_time`, `expire_time`, `pay_time`, `use_time`, `refund_time`, `update_time` FROM hmdp.tb_voucher_order WHERE user_id % 2 = 0 AND voucher_id % 2 = 0;

INSERT INTO hmdp_0.tb_voucher_order_1 (`id`, `user_id`, `voucher_id`, `pay_type`, `status`, `create_time`, `expire_time`, `pay_time`, `use_time`, `refund_time`, `update_time`)
SELECT `id`, `user_id`, `voucher_id`, `pay_type`, `status`, `create_time`, `expire_time`, `pay_time`, `use_time`, `refund_time`, `update_time` FROM hmdp.tb_voucher_order WHERE user_id % 2 = 0 AND voucher_id % 2 = 1;

INSERT INTO hmdp_1.tb_voucher_order_0 (`id`, `user_id`, `voucher_id`, `pay_type`, `status`, `create_time`, `expire_time`, `pay_time`, `use_time`, `refund_time`, `update_time`)
SELECT `id`, `user_id`, `voucher_id`, `pay_type`, `status`, `create_time`, `expire_time`, `pay_time`, `use_time`, `refund_time`, `update_time` FROM hmdp.tb_voucher_order WHERE user_id % 2 = 1 AND voucher_id % 2 = 0;

INSERT INTO hmdp_1.tb_voucher_order_1 (`id`, `user_id`, `voucher_id`, `pay_type`, `status`, `create_time`, `expire_time`, `pay_time`, `use_time`, `refund_time`, `update_time`)
SELECT `id`, `user_id`, `voucher_id`, `pay_type`, `status`, `create_time`, `expire_time`, `pay_time`, `use_time`, `refund_time`, `update_time` FROM hmdp.tb_voucher_order WHERE user_id % 2 = 1 AND voucher_id % 2 = 1;

INSERT INTO hmdp_0.tb_voucher_order_router_0 (order_id, user_id, voucher_id)
SELECT id, user_id, voucher_id FROM hmdp.tb_voucher_order WHERE id % 2 = 0 AND (id DIV 2) % 2 = 0;

INSERT INTO hmdp_0.tb_shop_voucher_index_0 (shop_id, voucher_id)
SELECT shop_id, id FROM hmdp.tb_voucher WHERE shop_id % 2 = 0 AND (shop_id DIV 2) % 2 = 0;

INSERT INTO hmdp_0.tb_voucher_order_router_1 (order_id, user_id, voucher_id)
SELECT id, user_id, voucher_id FROM hmdp.tb_voucher_order WHERE id % 2 = 0 AND (id DIV 2) % 2 = 1;

INSERT INTO hmdp_0.tb_shop_voucher_index_1 (shop_id, voucher_id)
SELECT shop_id, id FROM hmdp.tb_voucher WHERE shop_id % 2 = 0 AND (shop_id DIV 2) % 2 = 1;

INSERT INTO hmdp_1.tb_voucher_order_router_0 (order_id, user_id, voucher_id)
SELECT id, user_id, voucher_id FROM hmdp.tb_voucher_order WHERE id % 2 = 1 AND (id DIV 2) % 2 = 0;

INSERT INTO hmdp_1.tb_shop_voucher_index_0 (shop_id, voucher_id)
SELECT shop_id, id FROM hmdp.tb_voucher WHERE shop_id % 2 = 1 AND (shop_id DIV 2) % 2 = 0;

INSERT INTO hmdp_1.tb_voucher_order_router_1 (order_id, user_id, voucher_id)
SELECT id, user_id, voucher_id FROM hmdp.tb_voucher_order WHERE id % 2 = 1 AND (id DIV 2) % 2 = 1;

INSERT INTO hmdp_1.tb_shop_voucher_index_1 (shop_id, voucher_id)
SELECT shop_id, id FROM hmdp.tb_voucher WHERE shop_id % 2 = 1 AND (shop_id DIV 2) % 2 = 1;

INSERT INTO hmdp_0.tb_voucher_order_process_0 (order_id, user_id, voucher_id, status)
SELECT order_id, user_id, voucher_id, status FROM hmdp.tb_voucher_order_process
WHERE order_id % 2 = 0 AND (order_id DIV 2) % 2 = 0;

INSERT INTO hmdp_0.tb_voucher_order_process_1 (order_id, user_id, voucher_id, status)
SELECT order_id, user_id, voucher_id, status FROM hmdp.tb_voucher_order_process
WHERE order_id % 2 = 0 AND (order_id DIV 2) % 2 = 1;

INSERT INTO hmdp_1.tb_voucher_order_process_0 (order_id, user_id, voucher_id, status)
SELECT order_id, user_id, voucher_id, status FROM hmdp.tb_voucher_order_process
WHERE order_id % 2 = 1 AND (order_id DIV 2) % 2 = 0;

INSERT INTO hmdp_1.tb_voucher_order_process_1 (order_id, user_id, voucher_id, status)
SELECT order_id, user_id, voucher_id, status FROM hmdp.tb_voucher_order_process
WHERE order_id % 2 = 1 AND (order_id DIV 2) % 2 = 1;

COMMIT;
