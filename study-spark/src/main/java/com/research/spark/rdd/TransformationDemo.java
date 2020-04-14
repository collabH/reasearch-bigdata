package com.research.spark.rdd;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * @fileName: TransformationDemo.java
 * @description: 转换deom
 * @author: by echo huang
 * @date: 2020-04-14 16:17
 */
public class TransformationDemo {

    /**
     * 聚合转换
     */
    public void aggTran() {
        SparkConf sparkConf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext("local", "agg trans", sparkConf);

        JavaRDD<Tuple2<String, Integer>> rdd = sc.parallelize(Lists.newArrayList(new Tuple2<>("a", 1), new Tuple2<>("a", 2), new Tuple2<>("a", 3)));

        //reduceByKey
        rdd.keyBy(k -> k._1)
                .reduceByKey((v1, v2) -> Tuple2.apply(v1._1, v1._2 + v2._2))
                .collect()
                .stream()
                .forEach(System.out::print);

        //flodByKey
        rdd.keyBy(k -> k._1)
                .foldByKey(Tuple2.apply("a", 0), (v1, v2) -> Tuple2.apply(v1._1, v1._2 + v2._2))
                .collect()
                .stream()
                .forEach(System.out::print);

    }
}
