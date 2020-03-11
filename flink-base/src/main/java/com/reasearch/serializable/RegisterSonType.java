package com.reasearch.serializable;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @fileName: RegisterSonType.java
 * @description: Flink注册子类型
 * @author: by echo huang
 * @date: 2020-03-11 16:21
 */
public class RegisterSonType {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //内部使用TypeExtractor来根据子类信息创建对应的TypeInformation，然后根据TypeInformation类型交给pojo或者kryo来序列化
        env.registerType(Integer.class);
    }
}
