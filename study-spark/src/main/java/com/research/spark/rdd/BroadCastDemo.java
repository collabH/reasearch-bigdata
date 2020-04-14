package com.research.spark.rdd;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.reflect.ClassTag;

/**
 * @fileName: BroadCastDemo.java
 * @description: BroadCastDemo.java类说明
 * @author: by echo huang
 * @date: 2020-04-14 22:11
 */
public class BroadCastDemo {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf();
        JavaSparkContext sc = new JavaSparkContext("local", "broadcast", conf);
        Broadcast<Integer> broadcast = sc.broadcast(1);
        sc.parallelize(Lists.newArrayList(1,2,3)).map((k)->k+broadcast.value()).collect()
                .forEach(System.out::print);
    }
}
