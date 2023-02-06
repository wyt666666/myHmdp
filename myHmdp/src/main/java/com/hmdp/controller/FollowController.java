package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.service.IFollowService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/follow")
public class FollowController {

    @Resource
    private IFollowService followService;

    @PutMapping("/{id}/{isFollow}")
    public Result follow(@PathVariable("id") Long id,@PathVariable("isFollow") Boolean isFollow){
        followService.queryFollow(id,isFollow);
        return Result.ok();
    }

    @GetMapping("/or/not/{id}")
    public Result isFollow(@PathVariable("id") Long id){
        Boolean isFollow = followService.queryIsFollow(id);
        return Result.ok(isFollow);
    }

    @GetMapping("/common/{id}")
    public Result followCommons(@PathVariable("id") Long id){
        List<UserDTO> userDTOList = followService.queryFollowCommons(id);
        return Result.ok(userDTOList);
    }

}
