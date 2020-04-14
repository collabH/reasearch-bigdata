package com.research.spark.seriablier;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;

/**
 * @fileName: KryoSeriabler.java
 * @description: kryo序列化对象注册
 * @author: by echo huang
 * @date: 2020-04-14 21:55
 */
public class KryoSeriabler implements KryoRegistrator {
    @Override
    public void registerClasses(Kryo kryo) {
        //注册User类
        kryo.register(User.class);
        //在sparkConf中配置spark.kryo.registrator为com.research.spark.seriablier.KryoSeriabler
    }
}
