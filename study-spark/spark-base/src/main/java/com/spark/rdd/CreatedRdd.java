package com.spark.rdd;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @fileName: CreatedRdd.java
 * @description: CreatedRdd.java类说明
 * @author: by echo huang
 * @date: 2020-06-26 14:22
 */
public class CreatedRdd {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext("local", "createdRdd");
        JavaRDD<Integer> rdd = sc.parallelize(Lists.newArrayList(1, 2, 3, 4), 2);
        rdd.collect().forEach(System.out::println);

        JavaRDD<String> textRdd = sc.textFile("hdfs:///user/text.txt");
    }
}
