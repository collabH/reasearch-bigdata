/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.redis;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

/**
 * @fileName: RedisExtendClient.java
 * @description: jedis连接池扩展客户端
 * @author: by echo huang
 * @date: 2020-02-25 16:06
 */
public class RedisExtendClient {
    private ShardedJedis jedisPool;

    public RedisExtendClient(ShardedJedisPool jedisPool) {

        this.jedisPool = jedisPool.getResource();
    }

    public ShardedJedis getJedisPool() {
        return jedisPool;
    }

    public void setJedisPool(ShardedJedis jedisPool) {
        this.jedisPool = jedisPool;
    }
}
