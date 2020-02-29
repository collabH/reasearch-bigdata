/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.reasearch.redis;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @fileName: RedisModule.java
 * @description: RedisModule.java类说明
 * @author: by echo huang
 * @date: 2020-02-25 16:16
 */
public class RedisModule extends AbstractModule {
    private String jedisConfig = "redis-config.properties";

    @Override
    protected void configure() {
        //绑定配置属性
        Names.bindProperties(binder(), loadFile(jedisConfig, getClass()));
        //绑定redis
        bind(RedisExtendClient.class).toProvider(RedisClientProvider.class).in(Scopes.SINGLETON);
    }

    public static Properties loadFile(String prefix, Class<?> cla) {
        String fileName = prefix;
        Properties prop = new Properties();
        InputStream in = null;
        try {
            in = cla.getResource("/" + fileName).openStream();
            prop.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return prop;
    }

    @Test
    public void test() {
        RedisExtendClient instance = Guice.createInjector(new RedisModule())
                .getInstance(RedisExtendClient.class);
        System.out.println(instance.getJedisPool()
                .get("rerun:008c3e31"));
    }
}
