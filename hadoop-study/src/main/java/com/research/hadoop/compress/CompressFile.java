package com.research.hadoop.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @fileName: CompressFile.java
 * @description: 压缩文件使用
 * @author: by echo huang
 * @date: 2020-07-25 16:35
 */
public class CompressFile {

    private static String compressFile = "/Users/babywang/Desktop/input/order.txt";

    public void defleateCompress() throws IllegalAccessException, InstantiationException, IOException {
        FileInputStream fis = new FileInputStream(new File(compressFile));

        DefaultCodec codec = ReflectionUtils.newInstance(DefaultCodec.class, new Configuration());
        CompressionOutputStream cos = codec.createOutputStream(new FileOutputStream(new File(compressFile + codec.getDefaultExtension())));
        IOUtils.copyBytes(fis, cos, 1024 * 1024 * 5, false);
        IOUtils.closeStream(fis);
        IOUtils.closeStream(cos);
    }

    public void gzipCompress() throws Exception {
        FileInputStream fis = new FileInputStream(new File(compressFile));

        GzipCodec codec = ReflectionUtils.newInstance(GzipCodec.class, new Configuration());
        CompressionOutputStream cos = codec.createOutputStream(new FileOutputStream(new File(compressFile + codec.getDefaultExtension())));
        IOUtils.copyBytes(fis, cos, 1024 * 1024 * 5, false);
        IOUtils.closeStream(fis);
        IOUtils.closeStream(cos);

    }

    public void bzip2Compress() throws Exception {
        FileInputStream fis = new FileInputStream(new File(compressFile));

        BZip2Codec codec = ReflectionUtils.newInstance(BZip2Codec.class, new Configuration());
        CompressionOutputStream cos = codec.createOutputStream(new FileOutputStream(new File(compressFile + codec.getDefaultExtension())));
        IOUtils.copyBytes(fis, cos, 1024 * 1024 * 5, false);
        IOUtils.closeStream(fis);
        IOUtils.closeStream(cos);
    }

    public static void main(String[] args) throws Exception {
        CompressFile compressFile = new CompressFile();
        compressFile.bzip2Compress();
        compressFile.defleateCompress();
        compressFile.gzipCompress();

    }
}
