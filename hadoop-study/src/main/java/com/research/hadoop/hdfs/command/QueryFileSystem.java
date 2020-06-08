package com.research.hadoop.hdfs.command;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

/**
 * @fileName: QueryFileSystem.java
 * @description: QueryFileSystem.java类说明
 * @author: by echo huang
 * @date: 2020-03-18 01:45
 */
public class QueryFileSystem {


    public static void main(String[] args) throws Exception {
//        fileStatus();
//        listFileStatus();
        globStatus();
    }

    private static void fileStatus() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:8020/"), conf);
        FileStatus fileStatus = fs.getFileStatus(new Path("hdfs://localhost:8020/word.txt"));
        System.out.println(fileStatus);
    }


    private static void listFileStatus() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:8020/"), configuration);
        FileStatus[] fileStatuses = fs.listStatus(new Path("hdfs://localhost:8020/user"));
        System.out.println(Arrays.toString(fileStatuses));
    }

    private static void globStatus() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:8020/"), configuration);
        FileStatus[] fileStatuses = fs.globStatus(new Path("hdfs://localhost:8020/*/m*"));
        System.out.println(Arrays.toString(fileStatuses));
    }
}
