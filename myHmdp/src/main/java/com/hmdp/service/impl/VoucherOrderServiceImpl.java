package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIDWorker;
import com.hmdp.utils.StreamBlockingQueue;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hmdp.utils.RedisConstants.STREAM_ORDERS_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIDWorker redisIDWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private IVoucherOrderService voucherOrderService;
    private final static DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }
    private final static ExecutorService SECKILL_ORDER_EXECUTOR= Executors.newSingleThreadExecutor();
    //private final static BlockingQueue<VoucherOrder> orderTasks=new ArrayBlockingQueue<>(1024*1024);
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            StreamBlockingQueue streamBlockingQueue = new StreamBlockingQueue(seckillVoucherService,voucherOrderService,stringRedisTemplate);
            while (true) {
                    //基于Redis中的数据结构Stream实现阻塞队列处理消息
                List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().
                        read(Consumer.from("orderGroup", "consumer1"),
                        StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                        StreamOffset.create(STREAM_ORDERS_KEY, ReadOffset.lastConsumed()));
                if (list == null || list.isEmpty()){
                    continue;
                }
                try {
                    streamBlockingQueue.NormalQueueHandler(list);
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    //异常处理pending-list中的消息
                    while (true) {
                        List<MapRecord<String, Object, Object>> pendinglist = stringRedisTemplate.opsForStream().
                                read(Consumer.from("orderGroup", "consumer1"),
                                        StreamReadOptions.empty().count(1),
                                        StreamOffset.create(STREAM_ORDERS_KEY, ReadOffset.from("0")));
                        if (pendinglist == null || pendinglist.isEmpty()){
                            break;
                        }
                        try {
                            streamBlockingQueue.ExceptionQueueHandler(pendinglist);
                        }catch (Exception ex) {
                                log.error("处理订单异常",ex);
                            }
                    }
                }
            }
        }
    }
    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    //多线程异步执行秒杀操作
    @Override
    public Result secKillActivities(Long voucherId) {
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        if (seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀活动还未开始");
        }
        if (seckillVoucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀活动已经结束");
        }

        Long userId = UserHolder.getUser().getId();
        Long orderId = redisIDWorker.getRedisId("voucher_order");
        Long flag = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.singletonList(voucherId.toString()),
                userId.toString(),orderId.toString());
        if (flag.intValue() != 0){
            return Result.fail(flag==1?"库存不足":"无法重复购买");
        }
        return Result.ok(orderId);
    }

    @Override
    @Transactional
    public Result creatVoucherOrder(Long voucherId, Long userID) {
        return null;
    }

    //单线程同步执行秒杀操作
//    @Override
//    public Result secKillActivities(Long voucherId) {
//        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
//        if (seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀活动还未开始");
//        }
//        if (seckillVoucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀活动已经结束");
//        }
//        if (seckillVoucher.getStock()<1) {
//            return Result.fail("库存不足");
//        }
//        //解决一人一单问题,通过给重复下单用户的id加锁
//        //先提交事务 再释放锁
//        Long userID = UserHolder.getUser().getId();
//        //单台服务器下的锁机制->synchronized
////        synchronized (userID.toString().intern()) {
////            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
////            return proxy.creatVoucherOrder(voucherId, userID);
////        }
//        //多台服务器下的锁机制->自定义Redis分布式锁
////        RedisDistributedLock distributedLock=new RedisDistributedLock("order" + userID ,stringRedisTemplate);
////        boolean isLock = distributedLock.tryLock(1200L);
////        if (!isLock){
////            return Result.fail("无法重复购买");
////        }
////        try {
////            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
////            return proxy.creatVoucherOrder(voucherId, userID);
////        } finally {
////            distributedLock.unlock();
////        }
//        //多台服务器下的锁机制->分布式锁Redisson
////        RedisDistributedLock distributedLock=new RedisDistributedLock("order" + userID ,stringRedisTemplate);
//        RLock redissonlock = redissonClient.getLock(LOCK_REDISSON_KEY + userID);
//        boolean isLock = redissonlock.tryLock();
//        if (!isLock){
//            return Result.fail("无法重复购买");
//        }
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.creatVoucherOrder(voucherId, userID);
//        } finally {
//            redissonlock.unlock();
//        }
//    }
//
//    @Transactional
//    public Result creatVoucherOrder(Long voucherId, Long userID) {
//        Integer count = query().eq("user_id", userID).eq("voucher_id", voucherId).count();
//        if (count > 0){
//            return Result.fail("无法重复购买");
//        }
//        //解决超卖问题 使用乐观锁(改版)
//        boolean flag = seckillVoucherService.update().
//                    setSql("stock=stock-1").eq("voucher_id", voucherId).gt("stock",0).update();
//        if (!flag) {
//            return Result.fail("抢购失败");
//        }
//        VoucherOrder voucherOrder=new VoucherOrder();
//        Long orderId = redisIDWorker.getRedisId("voucher_order");
//        voucherOrder.setId(orderId);
//        voucherOrder.setVoucherId(voucherId);
//        voucherOrder.setUserId(userID);
//        save(voucherOrder);
//        return Result.ok(orderId);
//    }
}
