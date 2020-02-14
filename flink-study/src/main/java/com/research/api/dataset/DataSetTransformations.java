/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.api.dataset;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @fileName: DataSetTransformations.java
 * @description: DataSetTransformations.java类说明
 * @author: by echo huang
 * @date: 2020-02-14 16:34
 */
public class DataSetTransformations {

    private static final String fileText = "data/nested/d1/hello2.txt";

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Integer> list = Lists.newArrayList(1, 2, 3, 4, 5, 6, 1, 2, 4, 10, 34);
        DataSource<Integer> datasource = env.fromCollection(list);
        //map函数
        //mapFunction(env);
        //filter(datasource);
//        mapPartitionFuntion(env);
//        firstFunction(env);
        //flatMapFunction(env);
        //  distincFuntion(env);
//        joinFunction(env);
//        outJoinFunction(env);
//        crossFunction(env);
//        unionFunction(env);
//        rebalanceFunction(env);
//        hashParititionFunction(env);
        aggFunction(env);
    }

    /**
     * Map转换：map是对数据集里的每一个元素进行处理
     *
     * @param env
     * @throws Exception
     */
    private static void mapFunction(ExecutionEnvironment env) throws Exception {
        env.readTextFile(fileText)
                .map(new MyMapFunction())
                .print();
    }

    static
    class MyMapFunction implements MapFunction<String, String> {

        @Override
        public String map(String s) throws Exception {
            return "wx:" + s;
        }
    }

    /**
     * Fliter过滤:将满足条件的拿出来，不满足进行过滤
     */
    private static void filter(DataSource<Integer> datasource) throws Exception {
        datasource.filter(new MyFilter()).print();
    }

    static
    class MyFilter implements FilterFunction<Integer> {

        @Override
        public boolean filter(Integer val) throws Exception {
            return val > 5;
        }
    }

    /**
     * 根据指定的并行度去创建n个分区来执行
     *
     * @param env
     * @throws Exception
     */
    private static void mapPartitionFuntion(ExecutionEnvironment env) throws Exception {
        List<String> list = Lists.newArrayList();
        for (int i = 0; i < 100; i++) {
            list.add("people " + i);
        }
        //按照4个分区来执行
        env.fromCollection(list).setParallelism(4)
                .mapPartition(new MyMapPartitionFunction()).print();
    }

    static
    class MyMapPartitionFunction implements MapPartitionFunction<String, String> {

        @Override
        public void mapPartition(Iterable<String> iterable, Collector<String> collector) throws Exception {
            System.out.println("执行次数");
            iterable.forEach(collector::collect);
        }
    }

    /**
     * first：获取topn
     *
     * @param env
     * @throws Exception
     */
    private static void firstFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> list = Lists.newArrayList(new Tuple2<>(1, "Hadoop"),
                new Tuple2<>(1, "Spark"),
                new Tuple2<>(1, "Flink"),
                new Tuple2<>(2, "Java"),
                new Tuple2<>(2, "Linux"),
                new Tuple2<>(2, "Spring"),
                new Tuple2<>(3, "Vue"),
                new Tuple2<>(4, "R"));
        //打印钱三个
  /*      env.fromCollection(list)
                .first(3).print();*/

        //打印每一组top2
        env.fromCollection(list)
                .groupBy(0)
                .first(2).print();

        //打印每组top2，并且按照第二个字段升序
        env.fromCollection(list)
                .groupBy(0)
                .sortGroup(1, Order.ASCENDING)
                .first(2)
                .print();
    }

    /**
     * 一个元素生成多个元素
     *
     * @param env
     * @throws Exception
     */
    private static void flatMapFunction(ExecutionEnvironment env) throws Exception {
        List<String> list = Lists.newArrayList("Hadoop1 hello hh",
                "Hadoop1 hello1 hh",
                "Hadoop1 hello hh hello1");
        env.fromCollection(list).flatMap(new MyFlatMapFunction())
                .print();

    }

    static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {


        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] s1 = s.split(" ");
            for (String s2 : s1) {
                collector.collect(new Tuple2<>(s2, 1));
            }
        }
    }

    /**
     * 去重复
     *
     * @param env
     * @throws Exception
     */
    private static void distincFuntion(ExecutionEnvironment env) throws Exception {
        List<String> list = Lists.newArrayList("hello",
                "hello",
                "zhangsan");
        env.fromCollection(list)
                .distinct()
                .print();

    }


    /**
     * 俩个集合之间的join操作
     *
     * @param env
     * @throws Exception
     */
    private static void joinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> list = Lists.newArrayList(new Tuple2<>(1, "Hadoop"),
                new Tuple2<>(2, "Spark"),
                new Tuple2<>(3, "Flink"),
                new Tuple2<>(4, "Java"));

        List<Tuple2<Integer, String>> list2 = Lists.newArrayList(new Tuple2<>(1, "Good"),
                new Tuple2<>(2, "Cool"),
                new Tuple2<>(3, "H"),
                new Tuple2<>(5, "first"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(list);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(list2);
        data1.join(data2)
                .where(0)
                .equalTo(0)
                .with(new MyJoinFunction())
                .print();

    }

    /**
     * Join函数
     */
    static class MyJoinFunction implements JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, String> {

        @Override
        public String join(Tuple2<Integer, String> join1, Tuple2<Integer, String> join2) throws Exception {
            if (join1 == null) {
                return join2.f0 + ":" + join2.f1 + ":-";
            }
            if (join2 == null) {
                return join1.f0 + ":" + join1.f1 + ":-";
            }
            return join1.f0 + ":" + join1.f1 + ":" + join2.f1;
        }
    }

    /**
     * 外连接
     *
     * @param env
     * @throws Exception
     */
    private static void outJoinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> list = Lists.newArrayList(new Tuple2<>(1, "Hadoop"),
                new Tuple2<>(2, "Spark"),
                new Tuple2<>(3, "Flink"),
                new Tuple2<>(4, "Java"));

        List<Tuple2<Integer, String>> list2 = Lists.newArrayList(new Tuple2<>(1, "Good"),
                new Tuple2<>(2, "Cool"),
                new Tuple2<>(3, "H"),
                new Tuple2<>(5, "first"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(list);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(list2);
        //左外
        data1.leftOuterJoin(data2)
                .where(0)
                .equalTo(0)
                .with(new MyJoinFunction())
                .print();

        //右外
        data1.rightOuterJoin(data2)
                .where(0)
                .equalTo(0)
                .with(new MyJoinFunction())
                .print();
        //全外
        data1.fullOuterJoin(data2)
                .where(0)
                .equalTo(0)
                .with(new MyJoinFunction())
                .print();
    }

    /**
     * 笛卡尔积
     *
     * @param env
     * @throws Exception
     */
    private static void crossFunction(ExecutionEnvironment env) throws Exception {
        List<String> list1 = Lists.newArrayList("湖人", "勇士");
        List<Integer> list2 = Lists.newArrayList(98, 100, 124);
        DataSource<String> data1 = env.fromCollection(list1);
        DataSource<Integer> data2 = env.fromCollection(list2);

        data1.cross(data2)
                .print();
    }

    /**
     * 并集
     *
     * @param env
     * @throws Exception
     */
    private static void unionFunction(ExecutionEnvironment env) throws Exception {
        List<String> list1 = Lists.newArrayList("湖人", "勇士");
        List<String> list2 = Lists.newArrayList("湖人", "勇士", "张三");
        DataSource<String> data1 = env.fromCollection(list1);
        DataSource<String> data2 = env.fromCollection(list2);

        data1.union(data2)
                .print();
    }

    private static void rebalanceFunction(ExecutionEnvironment env) throws Exception {
        List<String> list1 = Lists.newArrayList("湖人", "勇士");
        DataSource<String> data1 = env.fromCollection(list1);
        data1.rebalance()
                .map(new MyMapFunction()).print();
    }

    /**
     * hash分区
     *
     * @param env
     * @throws Exception
     */
    private static void hashParititionFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple1<String>> list1 = Lists.newArrayList(new Tuple1<>("hello world"),
                new Tuple1<>("hello world1"));
        DataSource<Tuple1<String>> data1 = env.fromCollection(list1);
        data1.partitionByHash("f0")
                .map(new MapFunction<Tuple1<String>, String>() {
                    @Override
                    public String map(Tuple1<String> stringTuple1) throws Exception {
                        return "wx:" + stringTuple1.f0;
                    }
                }).print();
    }

    /**
     * 聚合函数
     *
     * @param env
     * @throws Exception
     */
    private static void aggFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple3<Integer, Integer, Integer>> list = Lists.newArrayList(
                new Tuple3<>(1, 3, 4),
                new Tuple3<>(11, 34, 2),
                new Tuple3<>(131, 32, 64),
                new Tuple3<>(14, 3, 54)
        );

        env.fromCollection(list)
                .aggregate(Aggregations.MAX, 0)
                .aggregate(Aggregations.MIN, 2)
                .print();
    }
}
