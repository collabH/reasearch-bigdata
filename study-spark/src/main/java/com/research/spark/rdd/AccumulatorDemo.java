package com.research.spark.rdd;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

/**
 * @fileName: AccumulatorDemo.java
 * @description: AccumulatorDemo.java类说明
 * @author: by echo huang
 * @date: 2020-04-14 23:23
 */
public class AccumulatorDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext("local", "accumulator", conf);
        LongAccumulator counter = sc.sc().longAccumulator("counter");
        JavaRDD<Integer> parallelize = sc.parallelize(Lists.newArrayList(1, 2, 3, 4, 5));
        parallelize.foreach(counter::add);
        System.out.println(counter.value());
    }
}
