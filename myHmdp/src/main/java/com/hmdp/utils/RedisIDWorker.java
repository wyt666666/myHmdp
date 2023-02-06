package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIDWorker {

    private final StringRedisTemplate stringRedisTemplate;

    public RedisIDWorker(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate = stringRedisTemplate;
    }

    private final static long BEGIN_TIMESTAMP=1640995200L;
    private final static int COUNT_BTTS=32;

    //32位时间戳+32位计数器->64位无重复RedisId
    public long getRedisId(String keyPrefix){

        LocalDateTime now=LocalDateTime.now();
        long nowTimeStamp = now.toEpochSecond(ZoneOffset.UTC);
        long timeStamp = nowTimeStamp-BEGIN_TIMESTAMP;

        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        long serialNum = stringRedisTemplate.opsForValue().increment("inc" + keyPrefix + date);

        return timeStamp << COUNT_BTTS | serialNum;
    }


}
