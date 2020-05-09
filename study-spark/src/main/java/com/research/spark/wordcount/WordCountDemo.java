package com.research.spark.wordcount;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;

/**
 * @fileName: WordCountDemo.java
 * @description: WordCountDemo.java类说明
 * @author: by echo huang
 * @date: 2020-04-16 19:13
 */
public class WordCountDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext("local", "wordcount", sparkConf);

        sc.textFile("/Users/babywang/Documents/reserch/studySummary/bigdata/spark/spark.txt")
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                        List<Tuple2<String, Integer>> list = Lists.newArrayList();
                        String[] token = s.split(",");
                        for (String value : token) {
                            list.add(new Tuple2<>(value, 1));
                        }
                        return list.iterator();
                    }
                }).keyBy(t -> t._1)
                .reduceByKey((v1, v2) -> new Tuple2<>(v1._1, v1._2 + v2._2))
                .map(v -> v._2)
                .collect()
                .forEach(System.out::print);
        Thread.sleep(1000000000);
    }
}
