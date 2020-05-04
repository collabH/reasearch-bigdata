package com.spark.window;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @fileName: WindowFunction.java
 * @description: WindowFunction.java类说明
 * @author: by echo huang
 * @date: 2020-05-04 23:26
 */
public class WindowFunction {
    public static void main(String[] args) throws Exception{
        SparkConf sparkConf = new SparkConf().setAppName("window function").setMaster("local[4]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Duration.apply(1000));

        JavaReceiverInputDStream<String> dStream = streamingContext.socketTextStream("localhost", 10086);
        streamingContext.checkpoint("study-spark/spark-streaming/checkpoint");

        dStream.flatMap(s -> Arrays.asList(s.split(",")).iterator())
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKeyAndWindow(Integer::sum,Duration.apply(4000),Duration.apply(2000))
                .print();


        streamingContext.start();
        streamingContext.awaitTermination();

    }
}
