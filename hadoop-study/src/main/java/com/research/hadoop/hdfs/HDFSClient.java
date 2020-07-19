/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.hadoop.hdfs;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * @fileName: HDFSClient.java
 * @description: HDFSApp操作
 * @author: by echo huang
 * @date: 2020-02-09 19:36
 */
@Slf4j
public class HDFSClient {
    private static final String HADOOP_HDFS_PATH = "hdfs://hadoop:8020";
    private FileSystem fileSystem;
    private Configuration configuration;

    @Before
    public void setUp() throws Exception {
        log.info("初始化Hadoop环境");
        configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        //指定用户，拿到hdfs文件对象
        fileSystem = FileSystem.get(new URI(HADOOP_HDFS_PATH), configuration, "babywang");
    }

    @After
    public void tearDown() throws Exception {
        // 关闭配置
        configuration = null;
        fileSystem = null;
        log.info("关闭用户对象");
    }

    /**
     * 创建文件夹
     *
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/hdfsclient/hellowrod"));
    }

    /**
     * 创建文件
     *
     * @throws Exception
     */
    @Test
    public void create() throws Exception {
        FSDataOutputStream outputStream = fileSystem.create(new Path("/hdfsclient/hellowrod/a.txt"));
        outputStream.write("hello world".getBytes(Charset.defaultCharset().name()));
    }

    /**
     * 查看HDFS文件内容
     */
    @Test
    public void cat() throws Exception {
        FSDataInputStream open = fileSystem.open(new Path("/hdfsclient/hellowrod/a.txt"));
        log.info("read a.txt:{}", org.apache.commons.io.IOUtils.toString(open));
    }

    /**
     * 重命名
     *
     * @throws Exception
     */
    @Test
    public void rename() throws Exception {
        boolean rename = fileSystem.rename(new Path("/hdfsclient/hellowrod/a.txt"), new Path("/hdfsclient/hellowrod/b.txt"));
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
        fileSystem.copyFromLocalFile(new Path("/Users/babywang/Documents/reserch/dev/workspace/reasech-bigdata/hadoop-study/src/main/resources/word.txt"),
                new Path("/hdfsclient/hellowrod/word.txt"));
        //断点上传
//        BufferedInputStream in = new BufferedInputStream(new FileInputStream("/Users/babywang/Documents/reserch/dev/workspace/reasech-bigdata/hadoop-study/src/main/resources/word.txt"));
//        FSDataOutputStream out = fileSystem.create(new Path("/hdfsclient/test/word.txt"), new Progressable() {
//            public void progress() {
//                System.out.println("断点传送");
//            }
//        });
//        IOUtils.copyBytes(in, out, 4096);
    }

    /**
     * 下载hdfs文件
     *
     * @throws Exception
     */
    @Test
    public void copyToLocal() throws Exception {
        fileSystem.copyToLocalFile(new Path("/hdfsclient/hellowrod/word.txt"),
                new Path("/Users/babywang/Desktop/a.txt"));
    }

    @Test
    public void delete() {
        boolean delete = false;
        try {
            delete = fileSystem.delete(new Path("/hdfsclient"), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(delete ? "删除成功" : "删除失败");
    }

    /**
     * 查询文件列表
     *
     * @throws Exception
     */
    @Test
    public void listFiles() throws Exception {
        RemoteIterator<LocatedFileStatus> list = fileSystem.listFiles(new Path("/wordcount.txt"), true);
        while (list.hasNext()) {
            LocatedFileStatus locatedFileStatus = list.next();
            //文件名
            System.out.println(locatedFileStatus.getPath().getName());

            //获取快信息
            for (BlockLocation blockLocation : locatedFileStatus.getBlockLocations()) {
                System.out.println(Arrays.toString(blockLocation.getCachedHosts()));
                System.out.println(Arrays.toString(blockLocation.getHosts()));
                System.out.println(Arrays.toString(blockLocation.getNames()));
                System.out.println(Arrays.toString(blockLocation.getStorageTypes()));
            }
        }
    }

}
