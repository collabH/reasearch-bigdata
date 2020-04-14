package com.research.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * @fileName: JavaMaxTemperature.java
 * @description: JavaMaxTemperature.java类说明
 * @author: by echo huang
 * @date: 2020-04-14 10:06
 */
public class JavaMaxTemperature {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();

        JavaSparkContext context = new JavaSparkContext("local", "MaxTemperatureSpark", sparkConf);

        JavaRDD<String> lines = context.textFile("/Users/babywang/Documents/reserch/studySummary/bigdata/spark/spark.txt");

        lines
                .map(value -> value.split(","))
                .filter(value -> Arrays.stream(value).findFirst().map("0"::equals).orElse(true))
                .saveAsTextFile("/Users/babywang/Documents/reserch/studySummary/bigdata/spark/output.txt");

    }
}
