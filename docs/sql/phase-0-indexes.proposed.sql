-- hmdp: proposed indexes for the current single-database schema.
-- Design artifact only; this file has NOT been executed.
-- Review, back up, check existing index names, and apply once in a maintenance window.
-- ALTER TABLE statements implicitly commit; wrapping this file in a transaction
-- does not make the migration reversible.

-- 1. Read-only preflight: both results must be zero before adding unique keys.
-- If duplicates exist, reconcile them according to business rules; do not delete blindly.
SELECT COUNT(*) AS duplicate_order_pairs
FROM (
    SELECT user_id, voucher_id
    FROM tb_voucher_order
    GROUP BY user_id, voucher_id
    HAVING COUNT(*) > 1
) AS duplicated_orders;

SELECT COUNT(*) AS duplicate_follow_pairs
FROM (
    SELECT user_id, follow_user_id
    FROM tb_follow
    GROUP BY user_id, follow_user_id
    HAVING COUNT(*) > 1
) AS duplicated_follows;

-- Check this output before running the non-idempotent ALTER statements below.
SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE,
       GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS indexed_columns
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
GROUP BY TABLE_NAME, INDEX_NAME, NON_UNIQUE
ORDER BY TABLE_NAME, INDEX_NAME;

-- 2. Required constraints and indexes for already implemented business paths.
-- The order constraint assumes the CURRENT rule: one order per user per voucher,
-- even after cancellation. Revise the business model first if this rule changes.
ALTER TABLE tb_voucher_order
    ADD UNIQUE KEY uk_order_user_voucher (user_id, voucher_id);

ALTER TABLE tb_follow
    ADD UNIQUE KEY uk_follow_user_target (user_id, follow_user_id),
    ADD KEY idx_follow_target_user (follow_user_id, user_id);

ALTER TABLE tb_voucher
    ADD KEY idx_voucher_shop_status (shop_id, status);

-- Pair these indexes with explicit query sorting for deterministic pagination:
-- author feed: ORDER BY create_time DESC, id DESC;
-- hot feed: ORDER BY liked DESC, id DESC.
-- The liked index also adds write work whenever the like counter changes.
ALTER TABLE tb_blog
    ADD KEY idx_blog_user_created (user_id, create_time DESC, id DESC),
    ADD KEY idx_blog_liked (liked DESC, id DESC);

-- 3. NOT enabled now: add only when a user order-list endpoint is implemented.
-- ALTER TABLE tb_voucher_order
--     ADD KEY idx_order_user_created (user_id, create_time DESC, id DESC);

-- Existing indexes that should not be duplicated:
-- tb_user: UNIQUE uniqe_key_phone(phone)
-- tb_shop: foreign_key_type(type_id)
-- tb_user_info: PRIMARY KEY(user_id)

-- After splitting orders, keep uk_order_user_voucher on every physical order table.
-- Always route by user_id. A local unique index cannot protect records written
-- to different physical tables by an inconsistent routing implementation.
