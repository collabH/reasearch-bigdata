package com.spark.source;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @fileName: UseTestSourceHandler.java
 * @description: 使用测试数据测试Spark Streaming
 * @author: by echo huang
 * @date: 2020-04-25 15:47
 */
public class UseTestSourceHandler {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("testSource");


        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Duration.apply(5000));

        Queue<JavaRDD<String>> testQueue = new ArrayBlockingQueue<>(50);

    }
}
