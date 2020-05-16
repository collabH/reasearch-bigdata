package com.spark.source;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @fileName: NetCatSourceHandler.java
 * @description: netcat数据源处理
 * @author: by echo huang
 * @date: 2020-04-25 14:53
 */
public class NetCatSourceHandler {
    public static void main(String[] args) throws Exception{
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("netcatSource");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Duration.apply(1000));

        JavaReceiverInputDStream<String> dStream = javaStreamingContext.socketTextStream("localhost", 10086);


        dStream.print();


        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
