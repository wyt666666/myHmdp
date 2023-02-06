package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.LogicExpiration;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;

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

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Shop queryById(Long id) throws InterruptedException {
        //正常情况
        String shopJSON = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        if (StrUtil.isNotBlank(shopJSON)){
            //StrUtil检验的空为""、"\t"、null等
            return JSONUtil.toBean(shopJSON,Shop.class);
        }
        if (shopJSON!=null){
            return null;
        }
        Shop shop = this.getById(id);
        if (Objects.isNull(shop)){
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,"",CACHE_NULL_TTL);
            return null;
        }
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL);
        return shop;

        //模拟缓存穿透后缓存重建--使用互斥锁解决方案
//        String shopJSON = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
//        if (StrUtil.isNotBlank(shopJSON)){
//            return JSONUtil.toBean(shopJSON,Shop.class);
//        }
//        if (shopJSON!=null){
//            return null;
//        }
//        Shop shop = null;
//        MutexLock mutLock = new MutexLock(stringRedisTemplate);
//        try {
//            if (!mutLock.lock(id)){
//                Thread.sleep(50);
//                return queryById(id);
//            }
//            shop = this.getById(id);
//            //模拟缓存重建
//            Thread.sleep(100);
//            if (Objects.isNull(shop)){
//                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,"",CACHE_NULL_TTL);
//                return null;
//            }
//            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.SECONDS);
//
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        } finally {
//            mutLock.unlock(id);
//        }
//        return shop;
        //模拟缓存穿透后缓存重建--使用逻辑过期方式解决方案
//        String LogicExceptionJSON = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
//        if (StrUtil.isBlank(LogicExceptionJSON)){
//            return null;
//        }
//        LogicExpiration logicExpiration = JSONUtil.toBean(LogicExceptionJSON, LogicExpiration.class);
//        JSONObject data = (JSONObject) logicExpiration.getData();
//        Shop shop = JSONUtil.toBean(data , Shop.class);
//        LocalDateTime expiration = logicExpiration.getExpiration();
//        if (expiration.isAfter(LocalDateTime.now())){
//            return shop;
//        }
//        MutexLock mutLock = new MutexLock(stringRedisTemplate);
//        if (!mutLock.lock(id)){
//            ExecutorService cache_executor= Executors.newFixedThreadPool(10);
//            cache_executor.submit(()->{
//                try {
//                    extracted(id);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                } finally {
//                    mutLock.unlock(id);
//                }
//            });
//        }
//        return shop;
    }

    public void extracted(Long id) throws InterruptedException {
        Shop shopById = this.getById(id);
        Thread.sleep(200);
        LogicExpiration logic=new LogicExpiration(LocalDateTime.now().plusSeconds(20),shopById);
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(logic));
    }

    @Override
    @Transactional
    public void updateByShop(Shop shop) {
        this.updateById(shop);
        stringRedisTemplate.delete(CODE_SHOP_KEY + shop.getId());
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.判断是否需要根据坐标查询
        if (x == null || y == null) {
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }

        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询redis、按照距离排序、分页。结果：shopId、distance
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo() // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
                .search(
                        SHOP_GEO_KEY + typeId,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        // 4.解析出id
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            // 没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }
        // 4.1.截取 from ~ end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 4.2.获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4.3.获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        // 5.根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6.返回
        return Result.ok(shops);
    }
}
