package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.spring.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    // 延迟注入
    @Resource
    private ObjectProvider<IVoucherOrderService> selfProvider;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    private static final int CONSUMER_COUNT = 10;

    private static final ExecutorService SECKILL_ORDER_EXECUTOR =
            Executors.newFixedThreadPool(CONSUMER_COUNT);

    // 多线程
    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        for (int i = 0; i < CONSUMER_COUNT; i++) {
            SECKILL_ORDER_EXECUTOR.submit(
                    new VoucherOrderHandler("c" + i)
            );
        }
    }

    private class VoucherOrderHandler implements Runnable{

        private final String consumerName;

        public VoucherOrderHandler(String consumerName) {
            this.consumerName = consumerName;
        }


        @Override
        public void run() {
            while(true){
                try {
                    // 1. 获取消息队列中的订单信息
                    List<@NonNull MapRecord<String, Object, Object>> mapRecords = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", consumerName),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create("stream.orders", ReadOffset.lastConsumed())
                    );
                    // 2. 判断是否获取到订单
                    if (mapRecords == null || mapRecords.isEmpty()) {
                        continue;
                    }
                    // 3. 有订单，解析订单信息
                    MapRecord<String, Object, Object> record = mapRecords.getFirst();
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 4. 创建订单成功后 才 ACK
                    boolean success = handlerVoucherOrder(voucherOrder);
                    if (success) {
                        stringRedisTemplate.opsForStream()
                                .acknowledge("stream.orders", "g1", record.getId());
                    }
                } catch (Exception e) {
                    handlePendingList(consumerName);
                    log.error("处理订单异常", e);
                }
            }
        }
    }

    private void handlePendingList(String consumerName) {
        while(true){
            try {
                // 1. 获取pending - list 中的订单信息
                List<@NonNull MapRecord<String, Object, Object>> mapRecords = stringRedisTemplate.opsForStream().read(
                        Consumer.from("g1", consumerName),
                        StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                        StreamOffset.create("stream.orders", ReadOffset.from("0"))
                );
                // 2. 判断是否获取到订单
                if (mapRecords == null || mapRecords.isEmpty()) {
                    // 2.1 如果获取失败，pending-list没有异常消息，直接退出
                    break;
                }
                // 3. 获取到消息
                MapRecord<String, Object, Object> record = mapRecords.getFirst();
                Map<Object, Object> value = record.getValue();
                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                // 4. 创建订单成功后 才 ACK，因为如果获取锁失败，不加以判断的话，可能会直接错误地确认该消息
                boolean success = handlerVoucherOrder(voucherOrder);
                if (success) {
                    stringRedisTemplate.opsForStream()
                            .acknowledge("stream.orders", "g1", record.getId());
                }
            } catch (Exception e) {
                log.error("处理订单异常", e);
            }
        }
    }


/*    private class VoucherOrderHandler implements Runnable{

        @Override
        public void run() {
            while(true){
                try {
                    VoucherOrder voucherOrder = orderTasks.take();
                    handlerVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }*/


    private boolean handlerVoucherOrder(VoucherOrder voucherOrder) {
        // 一人一单
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        // Redisson 分布式锁 同一个用户对同一张优惠券只能购买一次
        RLock lock = redissonClient.getLock("lock:order:" + voucherId + ":" + userId);
        boolean success = lock.tryLock();
        if(!success){
            // 获取锁失败，说明可能多次下单，直接返回错误
            log.error("不允许重复下单");
            return false;
        }
        // 获取锁成功，执行业务逻辑
        try {
            // 从 Spring 容器解析目标 Bean，获取代理对象
            IVoucherOrderService proxy = selfProvider.getObject();
            // 通过 AOP 代理调用方法，触发事务拦截器
            proxy.createVoucherOrder(voucherOrder);
            return true;
        } finally {
            // 最后释放锁
            lock.unlock();
        }

    }

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Override
    public Result secKillVoucher(Long voucherId) {
        // 1. 执行Lua脚本
        String userId = UserHolder.getUser().getId().toString();
        long orderId = redisIdWorker.nextId("order");
        Long res = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(), userId, String.valueOf(orderId));
        // 2. 获取返回值
        int r = res.intValue();
        // 2.1 返回值不为 0 返回错误信息
        if(r != 0){
            return Result.fail(r == 1 ? "库存不足":"一人只能购买一单");
        }
        // 3 返回值为 0, 下单成功，返回订单号
        return Result.ok(orderId);
    }

/*    @Override
    public Result secKillVoucher(Long voucherId) {
        // 1. 执行Lua脚本
        String userId = UserHolder.getUser().toString();
        Long res = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(), userId);
        // 2. 获取返回值
        int r = res.intValue();
        // 2.1 返回值不为 0 返回错误信息
        if(r != 0){
            return Result.fail(r == 1 ? "库存不足":"一人只能购买一单");
        }
        // 2.2 返回值为 0, 下单成功，返回订单号
        long orderId = redisIdWorker.nextId("order");
        // 3 订单加入阻塞队列
        // 3.1 新建订单对象
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(UserHolder.getUser().getId());
        voucherOrder.setVoucherId(voucherId);
        // 3.2 加入阻塞队列
        orderTasks.add(voucherOrder);

        // 4. 主线程内 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);
    }*/

    /*@Override
    public Result secKillVoucher(Long voucherId) {
        // 1. 获取优惠券信息
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        // 2. 判断秒杀是否开始
        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
            return Result.fail("秒杀尚未开始！");
        }
        // 3. 判断秒杀是否结束
        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
            return Result.fail("秒杀已经结束！");
        }
        // 4. 判断库存是否充足
        if(voucher.getStock() < 1){
            return Result.fail("库存不足!");
        }
        // 一人一单
        Long userId = UserHolder.getUser().getId();
        // SimpleRedisLock lock = new SimpleRedisLock(stringRedisTemplate,"order:" + userId);
        // Redisson 分布式锁 同一个用户对同一张优惠券只能购买一次
        RLock lock = redissonClient.getLock("lock:order:" + voucherId + ":" + userId);
        boolean success = lock.tryLock();
        if(!success){
            // 获取锁失败，说明可能多次下单，直接返回错误
            return Result.fail("一人只能下一单!");
        }
        // 获取锁成功，执行业务逻辑
        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            // 最后释放锁
            lock.unlock();
        }
    }*/

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 一人一单
        Long userId = voucherOrder.getUserId();
        long count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if(count > 0){
            log.error("用户已经购买过一次!");
            return;
        }
        // 5. 扣减库存，这里用到了Mysql的行锁
        boolean success = seckillVoucherService.update().setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock",0).update();
        if (!success){
            log.error("库存不足!");
            return;
        }
        // 6. 创建订单
        save(voucherOrder);
    }
}
