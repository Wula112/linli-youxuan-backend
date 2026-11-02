package com.hmdp.mq.producer;

import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.repository.redis.SeckillReservation;
import lombok.Getter;

/**
 * 秒杀事务消息的上下文：随半消息传入本地事务，回填 Lua 返回码供下单入口判断结果。
 *
 * <p>本地事务与发送在同一线程内同步执行，因此回填结果无需额外同步。
 */
@Getter
public class SeckillTxContext {

    private final VoucherOrderMessage message;
    private long result = SeckillReservation.UNKNOWN;

    public SeckillTxContext(VoucherOrderMessage message) {
        this.message = message;
    }

    public void complete(long result) {
        this.result = result;
    }
}
