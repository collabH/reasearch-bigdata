package com.research.spark.rdd;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @fileName: RddDemo.java
 * @description: Rdd创建
 * @author: by echo huang
 * @date: 2020-04-14 12:56
 */
public class RddDemo {


    /**
     * 基于本地内存创建RDD
     *
     * @param sc
     */
    public void createRdd1(JavaSparkContext sc) {
        //并行度由spark.default.parallelism属性指定，默认值为根据Spark的运行方式，本地运行时时
        //机器cpu和数，集群中时所有executor节点的内核总数，10是传递并行度可以覆盖默认配置
        JavaRDD<Integer> parallelize = sc.parallelize(Lists.newArrayList(1, 2, 3, 4, 5), 10);
        parallelize.collect()
                .forEach(System.out::println);
    }

    /**
     * 根据外部数据集的引用创建RDD
     * Spark使用MapReduce API的TextInputFormat来读取文件，文件分割也和HDFS相同，每个HDFS的Block对应
     * 一个Spark分区。通过textFile第二个参数以请求特定的分割数量来更改。
     *
     * @param sc
     * @param inputPath
     */
    public void createRdd2(JavaSparkContext sc, String inputPath) {
        JavaRDD<String> javaRDD = sc.textFile(inputPath);
    }

    /**
     * 加载一个完整的文件，将每个文件都加载进行内存进行处理
     *
     * @param sc
     * @param inputPath
     */
    public void createRdd3(JavaSparkContext sc, String inputPath) {
        //最小分区数
        int minPartitions = 10;
        sc.wholeTextFiles(inputPath, minPartitions);
    }

    /**
     * 根据顺序文件创建RDD
     *
     * @param sc
     */
    public void createRddForSequenceFile(JavaSparkContext sc, String inputPath) {
        JavaPairRDD<String, String> rdd = sc.sequenceFile(inputPath, String.class, String.class);
    }

    /**
     * 从hadoop中创建RDD
     *
     * @param sc
     * @param inputPath
     */
    public void createRddForHadoop(JavaSparkContext sc, String inputPath) {
        //旧版本Mapreduce api
//        sc.hadoopFile(inputPath, TextInputFormat.class, Long.class, String.class);
//        sc.hadoopRDD();
        //新版本 mapreduce api
//        sc.newAPIHadoopFile();
//        sc.newAPIHadoopRDD()
    }
}
