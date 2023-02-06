package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import jodd.util.StringUtil;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKES_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IFollowService followService;

    @Override
    public Result queryBlogById(Long id) {
        Blog blog = getById(id);
        if (Objects.isNull(blog)){
            return Result.fail("文章拉取失败");
        }
        User user = userService.getById(blog.getUserId());
        blog.setIcon(user.getIcon());
        blog.setName(user.getNickName());
        Double score = stringRedisTemplate.opsForZSet().score(BLOG_LIKES_KEY + id, user.getId());
        blog.setIsLike(score != null);
        return Result.ok(blog);
    }

    @Override
    public void queryIsLike(Long id) {
        Blog blog = getById(id);
        Long userId = UserHolder.getUser().getId();
        if (userId == null) {
            //用户未登录 无需查询是否点赞
            return;
        }
        Double score = stringRedisTemplate.opsForZSet().score(BLOG_LIKES_KEY + id, userId.toString());
        if (score == null) {
            boolean flag = update().setSql("liked=liked+1").eq("id", id).update();
            if (flag) {
                stringRedisTemplate.opsForZSet().add(BLOG_LIKES_KEY + id,userId.toString(),System.currentTimeMillis());
            }
        } else {
            boolean flag = update().setSql("liked=liked-1").eq("id", id).update();
            if (flag) {
                stringRedisTemplate.opsForZSet().remove(BLOG_LIKES_KEY + id,userId.toString());
            }
        }
    }

    @Override
    public List<UserDTO> queryLikesUser(Long id) {
        Set<String> range = stringRedisTemplate.opsForZSet().range(BLOG_LIKES_KEY + id, 0, 4);
        if (range == null || range.isEmpty()){
            return null;
        }
        List<Long> ids = range.stream().map(Long::valueOf).collect(Collectors.toList());
        String strIds = StringUtil.join(ids, ",");
        List<User> users = userService.listByIds(ids);
        return users.stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class)).collect(Collectors.toList());
    }

    @Override
    public void saveAndFeedBlog(Blog blog) {
        // 获取登录用户
        Long userId = UserHolder.getUser().getId();
        blog.setUserId(userId);
        // 保存探店博文
        save(blog);
        List<Follow> fans = followService.query().eq("follow_user_id", userId).list();
        fans.stream().map(Follow::getUserId).forEach(fanId->stringRedisTemplate.opsForZSet().
                add(FEED_KEY + fanId,blog.getId().toString(),System.currentTimeMillis()));
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        // 1.获取当前用户
        Long userId = UserHolder.getUser().getId();
        // 2.查询收件箱 ZREVRANGEBYSCORE key Max Min LIMIT offset count
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(FEED_KEY + userId, 0, max, offset, 2);
        // 3.非空判断
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }
        // 4.解析数据：blogId、minTime（时间戳）、offset
        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        int os = 1;
        for (ZSetOperations.TypedTuple<String> tuple : typedTuples) {
            // 4.1.获取id
            ids.add(Long.valueOf(tuple.getValue()));
            // 4.2.获取分数(时间戳）
            long time = tuple.getScore().longValue();
            if(time == minTime){
                os++;
            }else{
                minTime = time;
                os = 1;
            }
        }
        os = minTime == max ? os : os + offset;
        // 5.根据id查询blog
        String idStr = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Blog blog : blogs) {
            // 5.1.查询blog有关的用户
            User user = userService.getById(blog.getUserId());
            blog.setIcon(user.getIcon());
            blog.setName(user.getNickName());
            // 5.2.查询blog是否被点赞
            Double score = stringRedisTemplate.opsForZSet().score(BLOG_LIKES_KEY + userId, user.getId());
            blog.setIsLike(score != null);
        }
        // 6.封装并返回
        return Result.ok(new ScrollResult(blogs,minTime,os));
    }
}
