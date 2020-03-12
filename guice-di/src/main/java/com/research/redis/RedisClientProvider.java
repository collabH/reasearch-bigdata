/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.redis;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

import java.util.Collections;
import java.util.Objects;

/**
 * @fileName: RedisClientProvider.java
 * @description: redis客户端
 * @author: by echo huang
 * @date: 2020-02-25 16:05
 */
public class RedisClientProvider implements Provider<RedisExtendClient> {

    @Inject
    @Named("redis.database")
    private Integer database;
    @Inject
    @Named("redis.host")
    private String host;
    @Inject
    @Named("redis.port")
    private Integer port;
    @Inject
    @Named("redis.password")
    private String password;
    @Inject
    @Named("redis.pool.max.active")
    private Integer maxActive;
    @Inject
    @Named("redis.pool.max.idle")
    private Integer maxIdle;
    @Inject
    @Named("redis.pool.max.wait")
    private Integer maxWait;
    @Inject
    @Named("redis.pool.min.idle")
    private Integer minIdle;
    @Inject
    @Named("redis.timeout")
    private Integer timeout;

    private static ShardedJedisPool shardedPool;

    /**
     * 得到jedis连接池
     *
     * @return {@link ShardedJedisPool}
     */
    private ShardedJedisPool getJedisPool() {
        if (Objects.isNull(shardedPool)) {
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(this.maxActive);
            config.setMaxWaitMillis(this.maxWait);
            config.setMaxIdle(this.maxIdle);
            config.setMinIdle(this.minIdle);
            JedisShardInfo info = new JedisShardInfo(this.host, this.port,
                    this.timeout);
            if (this.password != null && !this.password.isEmpty()) {
                info.setPassword(password);
            }
            shardedPool = new ShardedJedisPool(config, Collections.singletonList(info));
        } else {
            return shardedPool;
        }
        return shardedPool;
    }

    @Override
    public RedisExtendClient get() {
        return new RedisExtendClient(getJedisPool());
    }
}
