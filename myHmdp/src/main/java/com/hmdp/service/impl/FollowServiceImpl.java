package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.FOLLOW_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IUserService userService;

    @Override
    public void queryFollow(Long id, Boolean isFollow) {
        Long userId = UserHolder.getUser().getId();
        if(isFollow){
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(id);
            save(follow);
            stringRedisTemplate.opsForSet().add(FOLLOW_KEY + id,userId.toString());
        } else {
            remove(new QueryWrapper<Follow>().eq("user_id",userId).eq("follow_user_id",id));
        }

    }

    @Override
    public Boolean queryIsFollow(Long id) {
        Long userId = UserHolder.getUser().getId();
        Integer count = query().eq("user_id", userId).eq("follow_user_id", id).count();
        return count > 0;
    }

    @Override
    public List<UserDTO> queryFollowCommons(Long id) {
        Long userId = UserHolder.getUser().getId();
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(FOLLOW_KEY + id, FOLLOW_KEY + userId);
        if (intersect == null || intersect.isEmpty()){
            return null;
        }
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());

        return userService.listByIds(ids).stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class)).collect(Collectors.toList());
    }
}
