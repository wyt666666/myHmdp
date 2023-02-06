package com.hmdp.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
public class LogicExpiration {

    public LocalDateTime expiration;
    public Object data;

}
