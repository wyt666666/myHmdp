package com.hmdp.extraUtil;

import cn.hutool.core.util.BooleanUtil;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_TTL;

public class MutexLock {

    private final StringRedisTemplate stringRedisTemplate;

    public MutexLock(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public boolean lock(Long id){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(LOCK_SHOP_KEY + id, "1", LOCK_SHOP_TTL, TimeUnit.MILLISECONDS);
        return BooleanUtil.isTrue(flag);
    }

    public void unlock(Long id){
        stringRedisTemplate.delete(LOCK_SHOP_KEY + id);
    }


}
