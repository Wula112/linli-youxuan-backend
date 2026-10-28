package com.hmdp.cache;

import com.hmdp.common.constant.RedisConstants;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.codec.StringCodec;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.hmdp.common.constant.RedisConstants.*;

@Component
@Slf4j
public class ShopBloomFilter implements ApplicationRunner {

    private static final int LOAD_BATCH_SIZE = 1000;

    private final ShopMapper shopMapper;
    private final RBloomFilter<Long> bloomFilter;
    private final RBucket<String> bloomData;
    private final RBucket<String> ready;

    public ShopBloomFilter(ShopMapper shopMapper, RedissonClient redissonClient) {
        this.shopMapper = shopMapper;
        this.bloomFilter = redissonClient.getBloomFilter(BLOOM_SHOP_KEY, LongCodec.INSTANCE);
        this.bloomData = redissonClient.getBucket(BLOOM_SHOP_KEY, StringCodec.INSTANCE);
        this.ready = redissonClient.getBucket(BLOOM_SHOP_READY_KEY, StringCodec.INSTANCE);
    }

    // 启动时预热布隆过滤器，只查询商铺id，避免一次加载全部商铺信息
    @Override
    public void run(ApplicationArguments args) {
        try {
            // 1. 预热完成前不拦截请求，避免已有商铺被误判为不存在
            ready.delete();
            bloomFilter.tryInit(BLOOM_SHOP_EXPECTED_INSERTIONS, BLOOM_SHOP_FALSE_PROBABILITY);
            // 0不是合法商铺id，仅用于保证空库预热后也存在过滤器位图
            bloomFilter.add(0L);
            long lastId = 0L;
            long count = 0L;
            while (true) {
                // 2. 按id分批加载，重复启动只补充数据，不清空原过滤器
                List<Shop> shops = shopMapper.selectList(new QueryWrapper<Shop>()
                        .select("id").gt("id", lastId).orderByAsc("id")
                        .last("LIMIT " + LOAD_BATCH_SIZE));
                if (shops.isEmpty()) {
                    break;
                }
                List<Long> ids = shops.stream().map(Shop::getId).collect(Collectors.toList());
                bloomFilter.add(ids);
                lastId = ids.get(ids.size() - 1);
                count += ids.size();
            }
            // 3. 所有商铺id加载完成后，才开启布隆过滤
            ready.set(UUID.randomUUID().toString());
            log.info("商铺布隆过滤器预热完成，共加载{}条商铺id", count);
        } catch (Exception e) {
            log.error("商铺布隆过滤器预热失败，暂时使用缓存空值方案", e);
        }
    }

    public boolean mightContain(Long id) {
        if (id == null || id <= 0) {
            return false;
        }
        try {
            // 未完成预热时，放行给原有查询逻辑处理
            String version = ready.get();
            if (version == null) {
                return true;
            }
            if (bloomFilter.contains(id)) {
                return true;
            }
            // Redisson在配置丢失时也可能返回false，不能直接判定商铺不存在
            bloomFilter.getSize();
            if (!bloomData.isExists()) {
                ready.delete();
                return true;
            }
            // 查询期间发生重建时，交给原有查询处理，避免使用旧判断结果
            return !version.equals(ready.get());
        } catch (Exception e) {
            log.warn("商铺布隆过滤器查询失败，放行查询，id={}", id, e);
            return true;
        }
    }

    // 新增商铺时同步加入过滤器；普通布隆过滤器不支持删除单个id
    public void add(Long id) {
        if (!bloomData.isExists()) {
            ready.delete();
        }
        if (bloomFilter.tryInit(BLOOM_SHOP_EXPECTED_INSERTIONS, BLOOM_SHOP_FALSE_PROBABILITY)) {
            // 运行期间过滤器丢失后重建，不能只靠新加入的id开启拦截
            ready.delete();
        }
        bloomFilter.add(id);
    }
}
