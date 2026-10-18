package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.spring.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    private final StringRedisTemplate stringRedisTemplate;

    public ShopServiceImpl(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    private static final ExecutorService CACHE_REBUILT_EXECUTOR = Executors.newFixedThreadPool(10);


    @Override
    public Result queryById(Long id) {
        // 1. 缓存击穿 互斥锁实现
        // Shop shop = queryWithMutex(id);
        // 2. 逻辑过期实现
        Shop shop = queryLogicalExpire(id);
        if (shop == null){
            return Result.fail("商铺不存在!");
        }
        return Result.ok(shop);
    }

    // 互斥锁实现缓存击穿
    public Shop queryWithMutex(Long id) {
        String key = CACHE_SHOP_KEY + id;

        // 1. 查缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        if (shopJson != null) {
            return null;
        }

        String lockKey = LOCK_SHOP_KEY + id;
        String lockValue = UUID.randomUUID().toString();
        boolean locked = false;

        try {
            // 2. 尝试获取锁
            locked = tryLock(lockKey, lockValue);
            if (!locked) {
                Thread.sleep(50);
                return queryWithMutex(id); // 关键：一定要 return
            }

            // 3. 拿到锁后二次检查缓存
            shopJson = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(shopJson)) {
                return JSONUtil.toBean(shopJson, Shop.class);
            }
            if (shopJson != null) {
                return null;
            }

            // 4. 查数据库
            Shop shop = getById(id);
            Thread.sleep(200);

            // 5. 数据库不存在，缓存空字符串防穿透
            if (shop == null) {
                stringRedisTemplate.opsForValue().set(
                        key, "", Expiration.from(CACHE_NULL_TTL,TimeUnit.MINUTES)
                );
                return null;
            }

            // 6. 写入缓存
            stringRedisTemplate.opsForValue().set(
                    key, JSONUtil.toJsonStr(shop),
                    Expiration.from(CACHE_SHOP_TTL,TimeUnit.MINUTES)
            );

            return shop;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            // 只有真正拿到锁的线程才释放
            if (locked) {
                unLock(lockKey);
            }
        }
    }


    private boolean tryLock(String key,String value){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, value, Expiration.seconds(10));
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

    // 解决缓存穿透
    public Shop queryWithPassThrough(Long id) {
        String key = CACHE_SHOP_KEY + id;
        // 1. 从redis查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2. 缓存命中，直接返回
        if(StrUtil.isNotBlank(shopJson)){
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        if(shopJson != null){
            return null;
        }
        // 3. 缓存未命中，查询数据库
        Shop shop = getById(id);
        // 4. 数据库也没有，设置空缓存对象，防缓存穿透
        if(shop == null){
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop), Expiration.from(RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES));
            return null;
        }
        // 5. 查到数据，写入Redis缓存
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop), Expiration.from(RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES));
        // 6. 返回数据
        return shop;
    }

    // 逻辑过期解决缓存击穿
    public Shop queryLogicalExpire(Long id){
        String key = CACHE_SHOP_KEY + id;
        // 1. 从redis查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2. 缓存未命中，直接返回
        if(StrUtil.isBlank(shopJson)){
            return null;
        }
        // 3. 缓存命中，查看缓存是否过期
        RedisData bean = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) bean.getData(), Shop.class);
        // 4. 没过期，直接返回
        if(bean.getExpireTime().isAfter(LocalDateTime.now())){
            return shop;
        }
        // 5. 过期，缓存重建
        // 5.1 尝试获取锁
        String lockKey = LOCK_SHOP_KEY + id;
        String lockValue = UUID.randomUUID().toString();
        if(tryLock(lockKey,lockValue)){
            // 5.2 二次校验在该线程获取锁时，缓存是否被其他线程已经重建
            shopJson = stringRedisTemplate.opsForValue().get(key);
            RedisData bean1 = JSONUtil.toBean(shopJson, RedisData.class);
            Shop shop1 = JSONUtil.toBean((JSONObject) bean1.getData(), Shop.class);
            if (bean1.getExpireTime().isAfter(LocalDateTime.now())) {
                return shop1;
            }
            // 5.3 交给其他线程重建缓存
            CACHE_REBUILT_EXECUTOR.submit(() -> {
                try {
                    this.save2ShopRedis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unLock(lockKey);
                }
            });
        }
        // 6 当前线程先返回旧数据
        return shop;
    }

    // 逻辑过期，重建缓存逻辑
    public void save2ShopRedis(Long id, Long expireSeconds) throws InterruptedException {
        Shop shop = getById(id);
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));

        // 写入缓存
        stringRedisTemplate.opsForValue().set(
                CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData)
        );
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        // 1. 查询数据库中是否有该商铺
        Long id = shop.getId();
        // 2. 有则更新，无则返回报错
        if(id == null){
            return Result.fail("商铺不存在！");
        }
        updateById(shop);
        // 3. 删除对应的缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        // 4. 返回结果
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1. 先判断传过来的有没有经纬度
        if(x == null || y == null){
            Page<Shop> shops = query().eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(shops.getRecords());
        }
        // 2. 计算分页参数，开始 - 结束坐标
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end  = current * SystemConstants.DEFAULT_PAGE_SIZE;
        // 3. 根据经纬度查询 附近周围店铺
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().search(key,
                GeoReference.fromCoordinate(x, y),
                new Distance(5000),
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end));
        if(results == null){
            return Result.ok(Collections.emptyList());
        }
        // 3.1 提取 [from,end）内的店铺ID
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> content = results.getContent();
        if(content.size() <= from){
            // 没有下一页，结束
            return Result.ok(Collections.emptyList());
        }
        List<Long> shopIds = new ArrayList<>(content.size());
        Map<String,Distance> distanceMap = new HashMap<>(content.size());
        content.stream().skip(from).forEach(result -> {
            // 3.2 获取店铺ID
            String shopId = result.getContent().getName();
            shopIds.add(Long.valueOf(shopId));
            // 3.3 获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopId,distance);
        });
        // 4. 根据ID查询Shop
        String shopIdStr = StrUtil.join(",", shopIds);
        List<Shop> list = query().in("id", shopIds).last("ORDER BY FIELD(id," + shopIdStr + ")").list();
        for (Shop shop : list) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        return Result.ok(list);
    }
}
