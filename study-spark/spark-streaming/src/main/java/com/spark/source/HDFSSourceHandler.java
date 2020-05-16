package com.spark.source;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @fileName: HDFSSourceHandler.java
 * @description: 读取hdfs数据
 * @author: by echo huang
 * @date: 2020-04-25 15:20
 */
public class HDFSSourceHandler {
    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setMaster("local[1]").setAppName("hdfsSource");

        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Duration.apply(5000));

        JavaDStream<String> stringJavaDStream = javaStreamingContext.textFileStream("hdfs://hadoop:8020/cache");


        stringJavaDStream.print();


        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
