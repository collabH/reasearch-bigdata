import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @fileName: JavaWordCount.java
 * @description: JavaWordCount.java类说明
 * @author: by echo huang
 * @date: 2020-06-25 23:04
 */
public class JavaWordCount {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext("local", "javaWordCount", new SparkConf());
        RDD<Tuple2<String, Integer>> rdd = sc.textFile("file:///Users/babywang/Documents/reserch/studySummary/bigdata/spark/spark.txt")
                .flatMap(a -> {
                    String[] split = a.split(",");
                    return Arrays.asList(split).iterator();
                }).map(v1 -> Tuple2.apply(v1, 1))
                .rdd();
        rdd.


    }
}
