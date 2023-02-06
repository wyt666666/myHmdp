package com.hmdp.extraUtil;


import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.BooleanUtil;
import lombok.AllArgsConstructor;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOCK_DISTRIBUTED_KEY;

@AllArgsConstructor
public class RedisDistributedLock {
    private String name;
    private StringRedisTemplate stringRedisTemplate;
    private final static String ID_PREFIX= UUID.randomUUID().toString(true) + "-";

    //加载lua资源
    private final static DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT =new DefaultRedisScript<Long>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }


    public boolean tryLock(Long timeOutSec){
        String threadID = ID_PREFIX + Thread.currentThread().getId();
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(LOCK_DISTRIBUTED_KEY + name, threadID, timeOutSec, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    // 基于Java代码实现的分布式锁的释放锁逻辑
//    public void unlock(){
//        String threadID = ID_PREFIX + Thread.currentThread().getId();
//        String lockID = stringRedisTemplate.opsForValue().get(LOCK_DISTRIBUTED_KEY + name);
//        if (threadID.equals(lockID)){
//            stringRedisTemplate.delete(LOCK_DISTRIBUTED_KEY + name);
//        }
//    }

    // 基于Lua脚本实现的分布式锁的释放锁逻辑
    public void unlock(){
        stringRedisTemplate.execute(UNLOCK_SCRIPT,
                Collections.singletonList(ID_PREFIX + Thread.currentThread().getId()),
                LOCK_DISTRIBUTED_KEY + name);
    }
}
