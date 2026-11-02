package com.hmdp.service.impl;

import com.baomidou.mybatisplus.spring.service.impl.ServiceImpl;
import com.hmdp.common.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.mq.producer.VoucherOrderPublisher;
import com.hmdp.repository.redis.SeckillReservation;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.core.incrementer.IdentifierGenerator;
import com.hmdp.entity.VoucherOrderRouter;
import com.hmdp.mapper.VoucherOrderRouterMapper;
import com.hmdp.common.context.UserHolder;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;

import java.io.Serializable;

import static com.hmdp.common.constant.RedisConstants.*;

// 原有实现使用的导入，随旧代码注释保留。
// import cn.hutool.core.bean.BeanUtil;
// import com.hmdp.entity.SeckillVoucher;
// import jakarta.annotation.PostConstruct;
// import org.jspecify.annotations.NonNull;
// import org.springframework.aop.framework.AopContext;
// import org.springframework.beans.factory.ObjectProvider;
// import org.springframework.boot.context.event.ApplicationReadyEvent;
// import org.springframework.context.event.EventListener;
// import org.springframework.data.redis.connection.stream.*;
// import java.time.Duration;
// import java.time.LocalDateTime;
// import java.util.Collections;
// import java.util.Map;
// import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
// 新增 RocketMQ 事务消息异步下单；原有实现注释保留，供对照。
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    // ===== 原有实现：Redis Stream、阻塞队列及同步下单历史代码，完整注释保留 =====
    //     @Resource
    //     private ISeckillVoucherService seckillVoucherService;
    //
    //     @Resource
    //     private VoucherMapper voucherMapper;
    //
    //     @Resource
    //     private ApplicationEventPublisher eventPublisher;
    //
    //     @Resource
    //     private RedisIdWorker redisIdWorker;
    //
    //     @Resource
    //     private StringRedisTemplate stringRedisTemplate;
    //
    //     @Resource
    //     private RedissonClient redissonClient;
    //
    //     // 延迟注入
    //     @Resource
    //     private ObjectProvider<IVoucherOrderService> selfProvider;
    //
    //     private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    //
    // //    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    //
    //     private static final int CONSUMER_COUNT = 10;
    //
    //     private static final ExecutorService SECKILL_ORDER_EXECUTOR =
    //             Executors.newFixedThreadPool(CONSUMER_COUNT);
    //
    //     // 多线程
    //     @EventListener(ApplicationReadyEvent.class)
    //     public void init() {
    //         for (int i = 0; i < CONSUMER_COUNT; i++) {
    //             SECKILL_ORDER_EXECUTOR.submit(
    //                     new VoucherOrderHandler("c" + i)
    //             );
    //         }
    //     }
    //
    //     private class VoucherOrderHandler implements Runnable{
    //
    //         private final String consumerName;
    //
    //         public VoucherOrderHandler(String consumerName) {
    //             this.consumerName = consumerName;
    //         }
    //
    //
    //         @Override
    //         public void run() {
    //             while(true){
    //                 try {
    //                     // 1. 获取消息队列中的订单信息
    //                     List<@NonNull MapRecord<String, Object, Object>> mapRecords = stringRedisTemplate.opsForStream().read(
    //                             Consumer.from("g1", consumerName),
    //                             StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
    //                             StreamOffset.create("stream.orders", ReadOffset.lastConsumed())
    //                     );
    //                     // 2. 判断是否获取到订单
    //                     if (mapRecords == null || mapRecords.isEmpty()) {
    //                         continue;
    //                     }
    //                     // 3. 有订单，解析订单信息
    //                     MapRecord<String, Object, Object> record = mapRecords.getFirst();
    //                     Map<Object, Object> value = record.getValue();
    //                     VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
    //                     // 4. 创建订单成功后 才 ACK
    //                     boolean success = handlerVoucherOrder(voucherOrder);
    //                     if (success) {
    //                         stringRedisTemplate.opsForStream()
    //                                 .acknowledge("stream.orders", "g1", record.getId());
    //                     }
    //                 } catch (Exception e) {
    //                     handlePendingList(consumerName);
    //                     log.error("处理订单异常", e);
    //                 }
    //             }
    //         }
    //     }
    //
    //     private void handlePendingList(String consumerName) {
    //         while(true){
    //             try {
    //                 // 1. 获取pending - list 中的订单信息
    //                 List<@NonNull MapRecord<String, Object, Object>> mapRecords = stringRedisTemplate.opsForStream().read(
    //                         Consumer.from("g1", consumerName),
    //                         StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
    //                         StreamOffset.create("stream.orders", ReadOffset.from("0"))
    //                 );
    //                 // 2. 判断是否获取到订单
    //                 if (mapRecords == null || mapRecords.isEmpty()) {
    //                     // 2.1 如果获取失败，pending-list没有异常消息，直接退出
    //                     break;
    //                 }
    //                 // 3. 获取到消息
    //                 MapRecord<String, Object, Object> record = mapRecords.getFirst();
    //                 Map<Object, Object> value = record.getValue();
    //                 VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
    //                 // 4. 创建订单成功后 才 ACK，因为如果获取锁失败，不加以判断的话，可能会直接错误地确认该消息
    //                 boolean success = handlerVoucherOrder(voucherOrder);
    //                 if (success) {
    //                     stringRedisTemplate.opsForStream()
    //                             .acknowledge("stream.orders", "g1", record.getId());
    //                 }
    //             } catch (Exception e) {
    //                 log.error("处理订单异常", e);
    //             }
    //         }
    //     }
    //
    //
    // /*    private class VoucherOrderHandler implements Runnable{
    //
    //         @Override
    //         public void run() {
    //             while(true){
    //                 try {
    //                     VoucherOrder voucherOrder = orderTasks.take();
    //                     handlerVoucherOrder(voucherOrder);
    //                 } catch (Exception e) {
    //                     log.error("处理订单异常", e);
    //                 }
    //             }
    //         }
    //     }*/
    //
    //
    //     private boolean handlerVoucherOrder(VoucherOrder voucherOrder) {
    //         // 一人一单
    //         Long userId = voucherOrder.getUserId();
    //         Long voucherId = voucherOrder.getVoucherId();
    //         // Redisson 分布式锁 同一个用户对同一张优惠券只能购买一次
    //         RLock lock = redissonClient.getLock("lock:order:" + voucherId + ":" + userId);
    //         boolean success = lock.tryLock();
    //         if(!success){
    //             // 获取锁失败，说明可能多次下单，直接返回错误
    //             log.error("不允许重复下单");
    //             return false;
    //         }
    //         // 获取锁成功，执行业务逻辑
    //         try {
    //             // 从 Spring 容器解析目标 Bean，获取代理对象
    //             IVoucherOrderService proxy = selfProvider.getObject();
    //             // 通过 AOP 代理调用方法，触发事务拦截器
    //             proxy.createVoucherOrder(voucherOrder);
    //             return true;
    //         } finally {
    //             // 最后释放锁
    //             lock.unlock();
    //         }
    //
    //     }
    //
    //     static {
    //         SECKILL_SCRIPT = new DefaultRedisScript<>();
    //         SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
    //         SECKILL_SCRIPT.setResultType(Long.class);
    //     }
    //
    //     @Override
    //     public Result secKillVoucher(Long voucherId) {
    //         // 1. 执行Lua脚本
    //         String userId = UserHolder.getUser().getId().toString();
    //         long orderId = redisIdWorker.nextId("order");
    //         Long res = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(), userId, String.valueOf(orderId));
    //         // 2. 获取返回值
    //         int r = res.intValue();
    //         // 2.1 返回值不为 0 返回错误信息
    //         if(r != 0){
    //             return Result.fail(r == 1 ? "库存不足":"一人只能购买一单");
    //         }
    //         // 3 返回值为 0, 下单成功，返回订单号
    //         return Result.ok(orderId);
    //     }
    //
    // /*    @Override
    //     public Result secKillVoucher(Long voucherId) {
    //         // 1. 执行Lua脚本
    //         String userId = UserHolder.getUser().toString();
    //         Long res = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(), userId);
    //         // 2. 获取返回值
    //         int r = res.intValue();
    //         // 2.1 返回值不为 0 返回错误信息
    //         if(r != 0){
    //             return Result.fail(r == 1 ? "库存不足":"一人只能购买一单");
    //         }
    //         // 2.2 返回值为 0, 下单成功，返回订单号
    //         long orderId = redisIdWorker.nextId("order");
    //         // 3 订单加入阻塞队列
    //         // 3.1 新建订单对象
    //         VoucherOrder voucherOrder = new VoucherOrder();
    //         voucherOrder.setId(orderId);
    //         voucherOrder.setUserId(UserHolder.getUser().getId());
    //         voucherOrder.setVoucherId(voucherId);
    //         // 3.2 加入阻塞队列
    //         orderTasks.add(voucherOrder);
    //
    //         // 4. 主线程内 获取代理对象
    //         proxy = (IVoucherOrderService) AopContext.currentProxy();
    //         return Result.ok(orderId);
    //     }*/
    //
    //     /*@Override
    //     public Result secKillVoucher(Long voucherId) {
    //         // 1. 获取优惠券信息
    //         SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
    //         // 2. 判断秒杀是否开始
    //         if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
    //             return Result.fail("秒杀尚未开始！");
    //         }
    //         // 3. 判断秒杀是否结束
    //         if(voucher.getEndTime().isBefore(LocalDateTime.now())){
    //             return Result.fail("秒杀已经结束！");
    //         }
    //         // 4. 判断库存是否充足
    //         if(voucher.getStock() < 1){
    //             return Result.fail("库存不足!");
    //         }
    //         // 一人一单
    //         Long userId = UserHolder.getUser().getId();
    //         // SimpleRedisLock lock = new SimpleRedisLock(stringRedisTemplate,"order:" + userId);
    //         // Redisson 分布式锁 同一个用户对同一张优惠券只能购买一次
    //         RLock lock = redissonClient.getLock("lock:order:" + voucherId + ":" + userId);
    //         boolean success = lock.tryLock();
    //         if(!success){
    //             // 获取锁失败，说明可能多次下单，直接返回错误
    //             return Result.fail("一人只能下一单!");
    //         }
    //         // 获取锁成功，执行业务逻辑
    //         try {
    //             IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
    //             return proxy.createVoucherOrder(voucherId);
    //         } finally {
    //             // 最后释放锁
    //             lock.unlock();
    //         }
    //     }*/
    //
    //     @Transactional
    //     public void createVoucherOrder(VoucherOrder voucherOrder) {
    //         // 一人一单
    //         Long userId = voucherOrder.getUserId();
    //         long count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
    //         if(count > 0){
    //             log.error("用户已经购买过一次!");
    //             return;
    //         }
    //         // 5. 扣减库存，这里用到了Mysql的行锁
    //         boolean success = seckillVoucherService.update().setSql("stock = stock - 1")
    //                 .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock",0).update();
    //         if (!success){
    //             log.error("库存不足!");
    //             return;
    //         }
    //         // 6. 创建订单
    //         save(voucherOrder);
    //         // 优惠券列表缓存包含库存，订单事务提交后让所属商铺的优惠券列表本地缓存失效。
    //         Voucher voucher = voucherMapper.selectById(voucherOrder.getVoucherId());
    //         if (voucher != null) {
    //             eventPublisher.publishEvent(CacheInvalidationMessage.voucherList(voucher.getShopId()));
    //         }
    //     }
    // ===== 原有实现保留结束；下方为新增 RocketMQ 事务消息实现 =====

    @Resource
    private SeckillReservation seckillReservation;
    @Resource
    private IdentifierGenerator identifierGenerator;
    @Resource
    private VoucherOrderRouterMapper voucherOrderRouterMapper;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private VoucherOrderPublisher voucherOrderPublisher;
    // 建单、支付与关单已拆成独立类，门面只做委托，事务边界留在各自实现里。
    @Resource
    private VoucherOrderCreateService voucherOrderCreateService;
    @Resource
    private VoucherOrderPaymentService voucherOrderPaymentService;
    @Resource
    private VoucherOrderCloseService voucherOrderCloseService;

    @Override
    public Result<Long> secKillVoucher(Long voucherId) {
        if (voucherId == null || voucherId <= 0) {
            return Result.fail("优惠券不存在");
        }
        Long userId = UserHolder.getUser().getId();
        long orderId = identifierGenerator.nextId(new VoucherOrder()).longValue();
        VoucherOrderMessage message = new VoucherOrderMessage(orderId, userId, voucherId);
        // 先发半消息，秒杀 Lua（扣库存、一人一单、写事务标记）在本地事务中执行，成功才提交消息
        long result = voucherOrderPublisher.sendInTransaction(message);
        if (result == SeckillReservation.SUCCESS) {
            // 只代表请求已受理，数据库订单由消费者创建
            return Result.ok(orderId);
        }
        if (result == SeckillReservation.STOCK_EMPTY) {
            return Result.fail("库存不足");
        }
        if (result == SeckillReservation.DUPLICATE) {
            return Result.fail("一人只能购买一单");
        }
        // 本地事务没有给出确定结果：回退预扣，避免库存被长期占用
        log.error("秒杀本地事务结果未知，尝试回退预扣，message={}", message);
        return rollbackReservation(message);
    }

    @Override
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        voucherOrderCreateService.createVoucherOrder(voucherOrder);
    }

    @Override
    public VoucherOrder getById(Serializable id) {
        if (id == null) return null;
        VoucherOrderRouter route = voucherOrderRouterMapper.selectById(id);
        return route == null ? null : baseMapper.selectRoutedOrder(
                route.getOrderId(), route.getUserId(), route.getVoucherId());
    }

    @Override
    public Result<VoucherOrder> queryVoucherOrder(Long orderId) {
        if (orderId == null || orderId <= 0) {
            return Result.fail("订单号不正确");
        }
        if (UserHolder.getUser() == null) {
            return Result.fail("请先登录");
        }
        VoucherOrder order = getById(orderId);
        if (order == null || !UserHolder.getUser().getId().equals(order.getUserId())) {
            return Result.fail("订单不存在");
        }
        return Result.ok(order);
    }

    @Override
    public Result<Long> mockPay(Long orderId, Integer payType) {
        return voucherOrderPaymentService.mockPay(orderId, payType);
    }

    @Override
    public void closeExpiredOrder(Long orderId) {
        voucherOrderCloseService.closeExpiredOrder(orderId);
    }

    private Result<Long> rollbackReservation(VoucherOrderMessage message) {
        RLock lock = redissonClient.getLock(LOCK_VOUCHER_ORDER_KEY + message.voucherId() + ":" + message.userId());
        boolean locked = false;
        try {
            // 与消费者共用锁，避免“回退预扣”和“数据库提交”并发执行。
            locked = lock.tryLock();
            if (!locked) {
                return pendingResult(message);
            }
            if (getById(message.orderId()) != null) {
                // 发布确认丢失，但消费者已完成订单，不能恢复库存。
                voucherOrderCreateService.clearPendingAfterCommit(new VoucherOrder().setId(message.orderId())
                        .setUserId(message.userId()).setVoucherId(message.voucherId()));
                return Result.ok(message.orderId());
            }
            // 原调用（保留）：未清理pending。
            // Long rolledBack = stringRedisTemplate.execute(ROLLBACK_SCRIPT, reservationKeys(message.voucherId()),
            //         message.userId().toString(), message.orderId().toString());
            Long rolledBack = seckillReservation.rollback(message.voucherId(), message.userId(), message.orderId());
            if (rolledBack == null || rolledBack < 0) {
                return pendingResult(message);
            }
            // 从当前的业务视角来讲，发送消息重试耗尽后，我们无法感知消息是否被消费
            // 只能先返回失败给调用方。
            return Result.fail("订单提交失败，请稍后重试");
        } catch (Exception ex) {
            log.error("秒杀预扣回退失败，需核对订单与预扣记录，message={}", message, ex);
            return pendingResult(message);
        } finally {
            if (locked) {
                lock.unlock();
            }
        }
    }

    private Result<Long> pendingResult(VoucherOrderMessage message) {
        log.error("秒杀订单状态待确认，保留预扣，message={}", message);
        return Result.fail("订单提交结果待确认，订单号：" + message.orderId());
    }
}
