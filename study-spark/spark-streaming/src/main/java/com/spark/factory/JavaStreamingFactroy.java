package com.spark.factory;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @fileName: JavaStreamingFactroy.java
 * @description: JavaStreamingFactroy.java类说明
 * @author: by echo huang
 * @date: 2020-04-25 17:16
 */
public abstract class JavaStreamingFactroy implements Function0<JavaStreamingContext> {

    public abstract JavaStreamingContext create();

    @Override
    public JavaStreamingContext call() throws Exception {
        return create();
    }
}
