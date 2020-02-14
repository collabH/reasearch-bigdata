/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.api.dataset;

import com.google.common.collect.Lists;
import com.mysql.cj.jdbc.Driver;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;

import java.util.List;

/**
 * @fileName: DataSetDataSource.java
 * @description: DataSetDataSource.java类说明
 * @author: by echo huang
 * @date: 2020-02-14 14:43
 */
public class DataSetDataSource {
    private static final String fileText = "file:///Users/babywang/Documents/reserch/学习总结/bigdata/flink/flink.txt";

    private static final String fileCsv = "/Users/babywang/Documents/reserch/dev/workspace/reasech-bigdata/data/test.csv";
    private static final String writeAsText = "/Users/babywang/Documents/reserch/dev/workspace/reasech-bigdata/data/test.txt";
    private static final String fileNested = "/Users/babywang/Documents/reserch/dev/workspace/reasech-bigdata/data/nested";
    private static final String fileCompressed = "/Users/babywang/Documents/reserch/dev/workspace/reasech-bigdata/data/nested/d2.zip";

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        fromCollection(env);
//        readTextFile(env);
//        readCsv(env);
//        readRecursiveDir(env);
//        readCompressedFile(env);
        //  readFromElements(env);
//        generateSequence(env);
        readFromMysql(env);
    }

    private static void fromCollection(ExecutionEnvironment env) throws Exception {
        List<String> list = Lists.newArrayList("hello world", "hello world", "hello world");
        env.fromCollection(list).print();
    }

    private static void readTextFile(ExecutionEnvironment env) throws Exception {
        env.readTextFile(fileText).print();
    }

    private static void readCsv(ExecutionEnvironment env) throws Exception {
        CsvReader csvReader = env.readCsvFile(fileCsv);
        csvReader.ignoreFirstLine()
                .pojoType(People.class, "name", "age", "desc")
                .print();
    }

    /**
     * 读递归嵌套文件夹
     */
    private static void readRecursiveDir(ExecutionEnvironment env) throws Exception {
        Configuration parameters = new Configuration();
        //设置递归读取
        parameters.setBoolean("recursive.file.enumeration", true);
        env.readTextFile(fileNested)
                .withParameters(parameters).print();
    }

    /**
     * Compression method	File extensions	Parallelizable
     * DEFLATE	.deflate	no
     * GZip	.gz, .gzip	no
     * Bzip2	.bz2	no
     * XZ	.xz	no
     */
    private static void readCompressedFile(ExecutionEnvironment env) throws Exception {
        env.readTextFile(fileCompressed).print();
    }

    /**
     * 读取元素
     *
     * @param env
     * @throws Exception
     */
    private static void readFromElements(ExecutionEnvironment env) throws Exception {
        env.fromElements("zhangsan", "wangwu").print();
    }

    /**
     * 生成序列号
     *
     * @param env
     * @throws Exception
     */
    private static void generateSequence(ExecutionEnvironment env) throws Exception {
        env.generateSequence(1, 100).setParallelism(1).print();
    }

    private static void readFromMysql(ExecutionEnvironment env) throws Exception {
        env.createInput(JDBCInputFormat.buildJDBCInputFormat()
                .setAutoCommit(true)
                .setDBUrl("jdbc:mysql://localhost:3306/ds0")
                .setPassword("root")
                .setUsername("root")
                .setDrivername(Driver.class.getName())
                .setQuery("select * from user")
                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
                .finish()).setParallelism(2)
                .print();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class People {
        private String name;
        private Integer age;
        private String desc;
    }
}
