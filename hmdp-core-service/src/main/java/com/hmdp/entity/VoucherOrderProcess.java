package com.hmdp.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.experimental.Accessors;

/** 一笔订单的建单仲裁与 Redis 预占释放进度。 */
@Data
@Accessors(chain = true)
@TableName("tb_voucher_order_process")
public class VoucherOrderProcess {
    @TableId(value = "order_id", type = IdType.INPUT)
    private Long orderId;
    private Long userId;
    private Long voucherId;
    private String status;
}
