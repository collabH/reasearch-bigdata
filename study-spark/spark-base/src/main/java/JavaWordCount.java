import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @fileName: JavaWordCount.java
 * @description: JavaWordCount.java类说明
 * @author: by echo huang
 * @date: 2020-06-25 23:04
 */
public class JavaWordCount {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext("yarn", "javaWordCount", new SparkConf());
        List<Tuple2<String, Integer>> collect = sc.textFile("file:///Users/babywang/Documents/reserch/studySummary/bigdata/spark/spark.txt")
                .flatMap(a -> {
                    String[] split = a.split(",");
                    return Arrays.asList(split).iterator();
                }).mapToPair(v1 -> new Tuple2<>(v1, 1))
                .reduceByKey(Integer::sum)
                .collect();
        collect.forEach(System.out::println);
    }
}
