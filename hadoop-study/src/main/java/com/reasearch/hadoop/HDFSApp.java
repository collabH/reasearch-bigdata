/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.reasearch.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.net.URI;
import java.nio.charset.Charset;

/**
 * @fileName: HDFSApp.java
 * @description: HDFSApp操作
 * @author: by echo huang
 * @date: 2020-02-09 19:36
 */
public class HDFSApp {
    private static final String HADOOP_HDFS_PATH = "hdfs://localhost:8020";
    private FileSystem fileSystem;
    private Configuration configuration;

    @Before
    public void setUp() throws Exception {
        System.out.println("HDFSAPP.setUp");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HADOOP_HDFS_PATH), configuration, "hadoop");
    }

    @After
    public void tearDown() throws Exception {
        configuration = null;
        fileSystem = null;
        System.out.println("HDFSAPP.tearDown");
    }

    /**
     * 创建文件夹
     *
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /**
     * 创建文件
     *
     * @throws Exception
     */
    @Test
    public void create() throws Exception {
        FSDataOutputStream outputStream = fileSystem.create(new Path("/hdfsapi/text/a.txt"));
        outputStream.write("hello world".getBytes(Charset.defaultCharset().name()));
    }

    /**
     * 查看HDFS文件内容
     */
    @Test
    public void cat() throws Exception {
        FSDataInputStream open = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(open, System.out, 1024);
    }

    /**
     * 重命名
     *
     * @throws Exception
     */
    @Test
    public void rename() throws Exception {
        boolean rename = fileSystem.rename(new Path("/hdfsapi/test/a.txt"), new Path("/hdfsapi/test/b.txt"));
        System.out.println(String.format("是否修改成功:%s", rename));
    }

    /**
     * 将本地文件拷贝到hdfs
     *
     * @throws Exception
     */
    @Test
    public void copy() throws Exception {
        //从本地拷贝到hdfs中
        fileSystem.copyFromLocalFile(new Path("/User/a.txt"),
                new Path("/hdfsapi/test/a.txt"));
        //断点上传
        BufferedInputStream in = new BufferedInputStream(new FileInputStream("test.txt"));
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/"), new Progressable() {
            public void progress() {
                System.out.println("断点传送");
            }
        });
        IOUtils.copyBytes(in, out, 4096);
    }

    /**
     * 下载hdfs文件
     *
     * @throws Exception
     */
    public void copyToLocal() throws Exception {
        fileSystem.copyFromLocalFile(new Path("/hdfsapi/test/a.txt"),
                new Path("/User/a.txt"));
    }

    /**
     * 查询文件列表
     *
     * @throws Exception
     */
    public void listFiles() throws Exception {
        RemoteIterator<LocatedFileStatus> list = fileSystem.listFiles(new Path("/hdfsapi/text/"), true);
        while (list.hasNext()) {
            System.out.println(list.next().isDirectory());
        }
    }
}
