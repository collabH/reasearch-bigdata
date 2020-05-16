package com.spark.stateful;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @fileName: StatefulWordCountSinkMySql.java
 * @description: 有状态计算将数据sink到mysql
 * @author: by echo huang
 * @date: 2020-04-25 17:48
 */
public class StatefulWordCountSinkMySql {
    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("statefulMysql");

        JavaStreamingContext javaStreamingContext = JavaStreamingContext.getOrCreate("study-spark/spark-streaming/checkpoint",
                () -> new JavaStreamingContext(sparkConf, Duration.apply(5000)));

        javaStreamingContext.checkpoint("study-spark/spark-streaming/checkpoint");
        JavaReceiverInputDStream<String> dStream = javaStreamingContext.socketTextStream("localhost", 10086);

        JavaPairDStream<String, Integer> source = dStream.flatMap(s -> Arrays.asList(s.split(",")).iterator())
                .mapToPair(s -> new Tuple2<>(s, 1))
                .updateStateByKey((Function2<List<Integer>, Optional<Integer>, Optional<Integer>>) (values, newState) -> {

                    int count = values.stream()
                            .reduce(Integer::sum)
                            .orElse(0)
                            + newState.orElse(0);
                    return Optional.of(count);
                });

        //sink to mysql
        source.print();

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
