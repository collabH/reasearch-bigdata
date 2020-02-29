/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.reasearch.nacos;

import lombok.Data;

/**
 * @fileName: RedisProperties.java
 * @description: RedisProperties.java类说明
 * @author: by echo huang
 * @date: 2020-02-25 20:11
 */
@Data
public class RedisProperties {
    private String host;
    private Integer port;
    private String password;
    private Integer database;
    private Integer maxIdle;
    private Integer maxWait;
    private Integer maxActive;
    private Integer minIdle;
    private Integer timeout;
    private ReRunProperties rerun;
}
