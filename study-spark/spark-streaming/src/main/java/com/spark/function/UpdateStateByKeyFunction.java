package com.spark.function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @fileName: UpdateStateByKeyFunction.java
 * @description: UpdateStateByKeyFunction.java类说明
 * @author: by echo huang
 * @date: 2020-04-25 16:24
 */
public class UpdateStateByKeyFunction {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("stateFunction");


        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Duration.apply(5000));

        JavaReceiverInputDStream<String> dStream = javaStreamingContext.socketTextStream("localhost", 10086);

        //手动设置checkpoint
        javaStreamingContext.checkpoint(".");

        dStream.flatMap(s -> {
            String[] split = s.split(",");
            return Arrays.asList(split).iterator();
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).updateStateByKey(new CustomUpdateFunction())
                .print();


        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
