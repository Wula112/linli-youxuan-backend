package com.hmdp.mq.producer;

import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.mq.support.MqMessageCodec;
import com.hmdp.repository.redis.SeckillReservation;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

class SeckillTransactionListenerTest {

    private final SeckillReservation reservation = mock(SeckillReservation.class);
    private final SeckillTransactionListener listener = new SeckillTransactionListener(reservation);
    private final VoucherOrderMessage order = new VoucherOrderMessage(1000L, 5L, 10L);

    @Test
    void shouldCommitWhenReservationSucceeds() {
        when(reservation.reserve(10L, 5L, 1000L)).thenReturn(SeckillReservation.SUCCESS);
        SeckillTxContext context = new SeckillTxContext(order);

        assertEquals(LocalTransactionState.COMMIT_MESSAGE, listener.executeLocalTransaction(message(), context));
        assertEquals(SeckillReservation.SUCCESS, context.getResult());
    }

    @Test
    void shouldRollbackWhenStockIsInsufficient() {
        when(reservation.reserve(10L, 5L, 1000L)).thenReturn(SeckillReservation.STOCK_EMPTY);
        SeckillTxContext context = new SeckillTxContext(order);

        assertEquals(LocalTransactionState.ROLLBACK_MESSAGE, listener.executeLocalTransaction(message(), context));
        assertEquals(SeckillReservation.STOCK_EMPTY, context.getResult());
    }

    @Test
    void shouldRollbackOnDuplicatePurchase() {
        when(reservation.reserve(10L, 5L, 1000L)).thenReturn(SeckillReservation.DUPLICATE);
        SeckillTxContext context = new SeckillTxContext(order);

        assertEquals(LocalTransactionState.ROLLBACK_MESSAGE, listener.executeLocalTransaction(message(), context));
        assertEquals(SeckillReservation.DUPLICATE, context.getResult());
    }

    @Test
    void shouldLeaveStateUnknownWhenLocalTransactionFails() {
        when(reservation.reserve(anyLong(), anyLong(), anyLong())).thenThrow(new IllegalStateException("redis unavailable"));
        SeckillTxContext context = new SeckillTxContext(order);

        assertEquals(LocalTransactionState.UNKNOW, listener.executeLocalTransaction(message(), context));
        assertEquals(SeckillReservation.UNKNOWN, context.getResult());
    }

    @Test
    void shouldCommitOnCheckBackOnlyWhenTransactionMarkerExists() {
        when(reservation.hasTransactionMarker(1000L)).thenReturn(true);
        assertEquals(LocalTransactionState.COMMIT_MESSAGE, listener.checkLocalTransaction(ext()));

        when(reservation.hasTransactionMarker(1000L)).thenReturn(false);
        assertEquals(LocalTransactionState.ROLLBACK_MESSAGE, listener.checkLocalTransaction(ext()));
    }

    @Test
    void shouldAskForAnotherCheckWhenMarkerCannotBeRead() {
        when(reservation.hasTransactionMarker(1000L)).thenThrow(new IllegalStateException("redis unavailable"));
        assertEquals(LocalTransactionState.UNKNOW, listener.checkLocalTransaction(ext()));
    }

    @Test
    void shouldRollbackUnreadableHalfMessage() {
        MessageExt broken = new MessageExt();
        broken.setBody("not-json".getBytes());
        assertEquals(LocalTransactionState.UNKNOW, listener.checkLocalTransaction(broken));
    }

    private Message message() {
        return new Message("topic", MqMessageCodec.encode(order));
    }

    private MessageExt ext() {
        MessageExt message = new MessageExt();
        message.setTopic("topic");
        message.setBody(MqMessageCodec.encode(order));
        message.setMsgId("msg-1");
        return message;
    }
}
