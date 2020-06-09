package com.research.hadoop.hdfs.compress;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.lang.reflect.Proxy;

/**
 * @fileName: CompressTest.java
 * @description: CompressTest.java类说明
 * @author: by echo huang
 * @date: 2020-06-08 19:44
 */
public class CompressTest extends Configured implements Tool {
    public void compress(String method, String fileName) throws Exception {
        File file = new File(fileName);
        FileInputStream fileInputStream = new FileInputStream(file);
        Class<?> codecClass = Class.forName(method);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, getConf());
        File fileOut = new File(fileName + codec.getDefaultExtension());
        fileOut.delete();
        FileOutputStream fileOutputStream = new FileOutputStream(fileOut);
        CompressionOutputStream outputStream = codec.createOutputStream(fileOutputStream);

    }

    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }
}
