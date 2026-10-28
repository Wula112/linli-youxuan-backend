package com.hmdp.service.impl;

import com.baomidou.mybatisplus.spring.service.impl.ServiceImpl;
import com.hmdp.common.dto.Result;
import com.hmdp.config.VoucherOrderProperties;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.common.enums.VoucherOrderStatus;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mq.message.CacheInvalidationMessage;
import com.hmdp.mq.message.VoucherOrderMessage;
import com.hmdp.mq.producer.VoucherOrderPublisher;
import com.hmdp.mq.producer.OrderTimeoutPublisher;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.core.incrementer.IdentifierGenerator;
import com.hmdp.entity.VoucherOrderRouter;
import com.hmdp.mapper.VoucherOrderRouterMapper;
import com.hmdp.common.context.UserHolder;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.retry.RetryException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

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
// 新增 RabbitMQ 异步下单；原有实现注释保留，供对照。
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
    // ===== 原有实现保留结束；下方为新增 RabbitMQ 实现 =====

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT = script("seckill.lua");
    private static final DefaultRedisScript<Long> ROLLBACK_SCRIPT = script("seckill-rollback.lua");
    private static final DefaultRedisScript<Long> PENDING_CLEAR_SCRIPT = script("seckill-pending-clear.lua");

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private VoucherMapper voucherMapper;
    @Resource
    private ApplicationEventPublisher eventPublisher;
    @Resource
    private IdentifierGenerator identifierGenerator;
    @Resource
    private VoucherOrderRouterMapper voucherOrderRouterMapper;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private VoucherOrderPublisher voucherOrderPublisher;
    @Resource
    private VoucherOrderProperties orderProperties;
    @Resource
    private OrderTimeoutPublisher orderTimeoutPublisher;
    @Resource
    private PlatformTransactionManager transactionManager;

    @Override
    public Result secKillVoucher(Long voucherId) {
        if (voucherId == null || voucherId <= 0) {
            return Result.fail("优惠券不存在");
        }
        Long userId = UserHolder.getUser().getId();
        long orderId = identifierGenerator.nextId(new VoucherOrder()).longValue();
        // 原调用（保留）：只记录预扣，没有待对账索引。
        // Long result = stringRedisTemplate.execute(SECKILL_SCRIPT, reservationKeys(voucherId),
        //         userId.toString(), Long.toString(orderId));
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT, reservationKeys(voucherId),
                userId.toString(), Long.toString(orderId), voucherId.toString(),
                Long.toString(System.currentTimeMillis()));
        if (result == null) {
            throw new IllegalStateException("秒杀预扣结果为空");
        }
        if (result != 0) {
            return Result.fail(result == 1 ? "库存不足" : "一人只能购买一单");
        }

        VoucherOrderMessage message = new VoucherOrderMessage(orderId, userId, voucherId);
        try {
            voucherOrderPublisher.publish(message);
            // 只代表请求已受理，数据库订单由消费者创建。
            return Result.ok(orderId);
        } catch (RetryException ex) {
            if (ex.getCause() instanceof Error error) {
                throw error;
            }
            if (ex.getCause() instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.error("秒杀订单消息发送失败，尝试回退预扣，message={}", message, ex);
            return rollbackReservation(message);
        }
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 幂等性，同一订单重复投递直接成功，ACK 丢失后重投也不会再次扣减库存。
        VoucherOrder existing = getById(voucherOrder.getId());
        if (existing != null) {
            if (!existing.getUserId().equals(voucherOrder.getUserId())
                    || !existing.getVoucherId().equals(voucherOrder.getVoucherId())) {
                throw new IllegalArgumentException("订单标识与已有订单不一致");
            }
            clearPendingAfterCommit(voucherOrder);
            return;
        }
        Object reservedOrderId = stringRedisTemplate.opsForHash().get(
                SECKILL_RESERVATION_KEY + voucherOrder.getVoucherId(), voucherOrder.getUserId().toString());
        if (!voucherOrder.getId().toString().equals(reservedOrderId)) {
            // 发送失败的回退已完成，（此时reservedOrderId应为Null）
            // 或发送失败的回退已完成，并且用户已经发起新请求，生成了新订单Id（reservedOrderId），此时旧消息带了旧订单Id。
            log.info("忽略已回退的秒杀订单，orderId={}", voucherOrder.getId());
            return;
        }
        // 原校验（保留）：历史已关闭订单也阻止再次购买。
        // long count = query().eq("user_id", voucherOrder.getUserId())
        //         .eq("voucher_id", voucherOrder.getVoucherId()).count();
        long count = query().eq("user_id", voucherOrder.getUserId())
                .eq("voucher_id", voucherOrder.getVoucherId())
                .ne("status", VoucherOrderStatus.CLOSED.getCode()).count();
        if (count > 0) {
            clearPendingAfterCommit(voucherOrder);
            return;
        }
        boolean success = seckillVoucherService.update().setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0).update();
        if (!success) {
            throw new IllegalStateException("数据库秒杀库存不足，voucherId=" + voucherOrder.getVoucherId());
        }
        // 只在首次建单时初始化；前面的幂等分支不会重置状态或延长支付时间。
        LocalDateTime createTime = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
        voucherOrder.setStatus(VoucherOrderStatus.UNPAID.getCode())
                .setCreateTime(createTime)
                .setExpireTime(createTime.plus(orderProperties.getPayTimeout()))
                .setPayTime(null);
        if (!save(voucherOrder)) {
            throw new IllegalStateException("秒杀订单保存失败，orderId=" + voucherOrder.getId());
        }
        // 与库存、订单处于同一个 XA 事务；只有订单号的入口通过目录补齐分片键。
        if (voucherOrderRouterMapper.insert(new VoucherOrderRouter().setOrderId(voucherOrder.getId())
                .setUserId(voucherOrder.getUserId()).setVoucherId(voucherOrder.getVoucherId())) != 1) {
            throw new IllegalStateException("订单路由保存失败，orderId=" + voucherOrder.getId());
        }
        Voucher voucher = voucherMapper.selectById(voucherOrder.getVoucherId());
        // 发送本地缓存失效消息
        if (voucher != null) {
            eventPublisher.publishEvent(CacheInvalidationMessage.voucherList(voucher.getShopId()));
        }
        clearPendingAfterCommit(voucherOrder);
        orderTimeoutPublisher.publishAfterCommit(voucherOrder.getId());
    }

    @Override
    public VoucherOrder getById(Serializable id) {
        if (id == null) return null;
        VoucherOrderRouter route = voucherOrderRouterMapper.selectById(id);
        return route == null ? null : baseMapper.selectRoutedOrder(
                route.getOrderId(), route.getUserId(), route.getVoucherId());
    }

    @Override
    public Result queryVoucherOrder(Long orderId) {
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
    public Result mockPay(Long orderId, Integer payType) {
        if (orderId == null || orderId <= 0) {
            return Result.fail("订单号不正确");
        }
        if (payType == null || payType < 1 || payType > 3) {
            return Result.fail("支付方式不正确");
        }
        if (UserHolder.getUser() == null) {
            return Result.fail("请先登录");
        }
        Long userId = UserHolder.getUser().getId();
        // 状态、归属和到期时间在同一条UPDATE中判断，数据库时间决定是否允许支付。
        VoucherOrderRouter route = voucherOrderRouterMapper.selectById(orderId);
        if (route == null || !userId.equals(route.getUserId())) {
            return Result.fail("订单不存在");
        }
        if (baseMapper.payIfUnpaid(orderId, userId, route.getVoucherId(), payType) == 1) {
            return Result.ok(orderId);
        }
        VoucherOrder order = baseMapper.selectRoutedOrder(orderId, route.getUserId(), route.getVoucherId());
        if (order == null || !userId.equals(order.getUserId())) {
            return Result.fail("订单不存在");
        }
        if (Integer.valueOf(VoucherOrderStatus.PAID.getCode()).equals(order.getStatus())) {
            // 重复支付按成功返回，不覆盖首次支付方式和支付时间。
            return Result.ok(orderId);
        }
        if (Integer.valueOf(VoucherOrderStatus.CLOSED.getCode()).equals(order.getStatus())) {
            return Result.fail("订单已关闭，无法支付");
        }
        if (Integer.valueOf(VoucherOrderStatus.UNPAID.getCode()).equals(order.getStatus())) {
            return Result.fail("订单已超时，无法支付");
        }
        return Result.fail("当前订单状态不支持支付");
    }

    @Override
    public void closeExpiredOrder(Long orderId) {
        if (orderId == null || orderId <= 0) {
            throw new IllegalArgumentException("订单号不正确");
        }
        VoucherOrderRouter route = voucherOrderRouterMapper.selectById(orderId);
        if (route == null) return;
        // 显式分开数据库事务和Redis补偿，确保不会在MySQL提交前释放购买资格。
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        VoucherOrder closed = transaction.execute(status -> {
            // 条件更新，status == 4 的订单才能关闭，幂等
            int updated = baseMapper.closeIfExpired(orderId, route.getUserId(), route.getVoucherId());
            VoucherOrder order = baseMapper.selectRoutedOrder(orderId, route.getUserId(), route.getVoucherId());
            if (updated == 1) {
                // 确认关闭订单，释放库存
                if (!seckillVoucherService.update().setSql("stock = stock + 1")
                        .eq("voucher_id", order.getVoucherId()).update()) {
                    throw new IllegalStateException("关单返还数据库库存失败，orderId=" + orderId);
                }
                Voucher voucher = voucherMapper.selectById(order.getVoucherId());
                if (voucher != null) {
                    eventPublisher.publishEvent(CacheInvalidationMessage.voucherList(voucher.getShopId()));
                }
            }
            // 重复消息也可以继续完成此前中断的Redis补偿；已支付、未到期或不存在则跳过。
            if (order == null || !Integer.valueOf(VoucherOrderStatus.CLOSED.getCode()).equals(order.getStatus())
                    || Boolean.TRUE.equals(order.getRedisStockRestored())) {
                return null;
            }
            return order;
        });
        if (closed == null) {
            return;
        }
        // 复用原归属校验Lua：只有Hash仍属于本订单才返库并释放资格，重复和迟到补偿均幂等。
        // 考虑这种情况：
        // 订单 O1 超时关闭，执行 Lua，成功释放了用户 A 对券 V 的购买资格。
        // 用户 A 马上又下了新订单 O2，Redis 里重新写入了 O2 的购买资格。
        // 此时 O1 的补偿消息因为“标记写入失败”被重试。
        // 如果 Lua 不校验归属，直接删除用户 A 的资格，就会把 O2 的资格误删掉。
        // 但 Lua 会校验：当前 Redis 里的 member 是不是 O1 的 member？
        // 发现是 O2 的 member，不是 O1，于是不动。
        String member = new VoucherOrderMessage(closed.getId(), closed.getUserId(), closed.getVoucherId()).pendingMember();
        Long restored = stringRedisTemplate.execute(ROLLBACK_SCRIPT, reservationKeys(closed.getVoucherId()),
                closed.getUserId().toString(), closed.getId().toString(), member);
        if (restored == null || restored < 0) {
            throw new IllegalStateException("关单返还Redis库存失败，等待重试，orderId=" + orderId);
        }
        // Lua成功但标记写入失败时，下一次重试补偿不会重复返库，也不会清理后续新订单的资格。
        transaction.executeWithoutResult(status -> baseMapper.markRedisStockRestored(
                orderId, route.getUserId(), route.getVoucherId()));
    }

    private Result rollbackReservation(VoucherOrderMessage message) {
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
                clearPendingAfterCommit(new VoucherOrder().setId(message.orderId())
                        .setUserId(message.userId()).setVoucherId(message.voucherId()));
                return Result.ok(message.orderId());
            }
            // 原调用（保留）：未清理pending。
            // Long rolledBack = stringRedisTemplate.execute(ROLLBACK_SCRIPT, reservationKeys(message.voucherId()),
            //         message.userId().toString(), message.orderId().toString());
            Long rolledBack = stringRedisTemplate.execute(ROLLBACK_SCRIPT, reservationKeys(message.voucherId()),
                    message.userId().toString(), message.orderId().toString(), message.pendingMember());
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

    private Result pendingResult(VoucherOrderMessage message) {
        log.error("秒杀订单状态待确认，保留预扣，message={}", message);
        return Result.fail("订单提交结果待确认，订单号：" + message.orderId());
    }

    private List<String> reservationKeys(Long voucherId) {
        // 原结构（保留）：库存 + 一人一单Set + 预占Hash。
        // return List.of(SECKILL_STOCK_KEY + voucherId, SECKILL_ORDER_KEY + voucherId,
        //         SECKILL_RESERVATION_KEY + voucherId);
        // 原Key列表（保留）：库存、预占和pending。新增补发计数供回退时一并清理。
        // return List.of(SECKILL_STOCK_KEY + voucherId, SECKILL_RESERVATION_KEY + voucherId, SECKILL_PENDING_KEY);
        return List.of(SECKILL_STOCK_KEY + voucherId, SECKILL_RESERVATION_KEY + voucherId,
                SECKILL_PENDING_KEY, SECKILL_PENDING_RETRIES_KEY);
    }

    private void clearPendingAfterCommit(VoucherOrder order) {
        String member = new VoucherOrderMessage(order.getId(), order.getUserId(), order.getVoucherId()).pendingMember();
        Runnable cleanup = () -> {
            try {
                // 资格Hash始终保留。清理失败由定时对账根据已提交订单再次清理。
                // 原清理：stringRedisTemplate.opsForZSet().remove(SECKILL_PENDING_KEY, member);
                stringRedisTemplate.execute(PENDING_CLEAR_SCRIPT,
                        List.of(SECKILL_PENDING_KEY, SECKILL_PENDING_RETRIES_KEY), member);
            } catch (Exception ex) {
                log.error("订单已提交但pending清理失败，等待对账重试，member={}", member, ex);
            }
        };
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    cleanup.run();
                }
            });
        } else {
            cleanup.run();
        }
    }

    private static DefaultRedisScript<Long> script(String name) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource(name));
        script.setResultType(Long.class);
        return script;
    }
}
