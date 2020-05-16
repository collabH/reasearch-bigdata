package com.spark.source;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @fileName: WordCountDemo.java
 * @description: WordCountDemo.java类说明
 * @author: by echo huang
 * @date: 2020-04-22 21:18
 */
public class WordCountDemo {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("TcpWordCount");
        //1秒一批次
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        JavaReceiverInputDStream<String> dStream = jssc.socketTextStream("localhost", 9999);


        //分割词汇
        JavaDStream<String> words = dStream.flatMap(x -> Arrays.asList(x.split(",")).iterator());

        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(Integer::sum);

        wordCounts.print();

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
