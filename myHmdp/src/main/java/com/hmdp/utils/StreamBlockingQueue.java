package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import lombok.AllArgsConstructor;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import java.util.List;
import java.util.Map;
import static com.hmdp.utils.RedisConstants.STREAM_ORDERS_KEY;

@AllArgsConstructor
public class StreamBlockingQueue {
    private ISeckillVoucherService seckillVoucherService;
    private IVoucherOrderService voucherOrderService;
    private StringRedisTemplate stringRedisTemplate;

    public void NormalQueueHandler(List<MapRecord<String, Object, Object>> list) {
        MapRecord<String, Object, Object> mapRecord = list.get(0);
        Map<Object, Object> mapVoucherOrder = mapRecord.getValue();
        VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(mapVoucherOrder, new VoucherOrder(), true);
        seckillVoucherService.update().setSql("stock=stock-1").gt("stock",0).
                eq("voucher_id", voucherOrder.getVoucherId()).update();
        voucherOrderService.save(voucherOrder);
        //进行ack消息确认
        stringRedisTemplate.opsForStream().acknowledge(STREAM_ORDERS_KEY,"orderGroup",mapRecord.getId());
    }

    public void ExceptionQueueHandler(List<MapRecord<String, Object, Object>> pendinglist) {
        MapRecord<String, Object, Object> pendingmapRecord = pendinglist.get(0);
        Map<Object, Object> mapVoucherOrder = pendingmapRecord.getValue();
        VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(mapVoucherOrder, new VoucherOrder(), true);
        seckillVoucherService.update().setSql("stock=stock-1").gt("stock",0).
                eq("voucher_id", voucherOrder.getVoucherId()).update();
        voucherOrderService.save(voucherOrder);
    }
}
