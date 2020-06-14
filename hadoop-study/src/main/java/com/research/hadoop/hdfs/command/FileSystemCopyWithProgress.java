package com.research.hadoop.hdfs.command;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.net.URI;

/**
 * @fileName: FileSystemCopyWithProgress.java
 * @description: FileSystemCopyWithProgress.java类说明
 * @author: by echo huang
 * @date: 2020-03-17 17:00
 */
@Slf4j
public class FileSystemCopyWithProgress {
    public static void main(String[] args) throws Exception {
//        append();
        if (log.isDebugEnabled()) {
            log.debug("test debug log");
        }
        create();
    }

    private static void create() throws Exception {
        String path = FileSystemCopyWithProgress.class.getClassLoader().getResource("word.txt").getPath();
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(path));
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:8020/word.txt"), conf);
        FSDataOutputStream out = fs.create(new Path("hdfs://localhost:8020/word.txt"), () -> System.out.println("datanode 回馈"));
        IOUtils.copyBytes(in, out, 4096, true);
    }

    private static void append() throws Exception {
        String path = FileSystemCopyWithProgress.class.getClassLoader().getResource("word.txt").getPath();
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(path));
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:8020/word.txt"), conf);
        FSDataOutputStream out = fs.append(new Path("hdfs://localhost:8020/word.txt"), 4096, () -> System.out.println("datanode 回馈"));
        IOUtils.copyBytes(in, out, 4096, true);
    }
}
