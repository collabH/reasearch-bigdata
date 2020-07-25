package com.research.hadoop.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @fileName: CompressFile.java
 * @description: 解压
 * @author: by echo huang
 * @date: 2020-07-25 16:35
 */
public class DeCompressFile {

    private static String compressFilePath = "/Users/babywang/Desktop/input/order.txt";
    private static String compressFile = "/Users/babywang/Desktop/input/order.txt.deflate";

    public void defleateDeCompress() throws IllegalAccessException, InstantiationException, IOException {
        //拿到压缩工程
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());

        CompressionCodec codec = factory.getCodec(new Path(compressFile));
        if (codec == null) {
            System.out.println("该文件压缩格式不存在");
            return;
        }

        CompressionInputStream cis = codec.createInputStream(new FileInputStream(new File(compressFilePath + codec.getDefaultExtension())));

        FileOutputStream fos = new FileOutputStream(new File(compressFilePath + ".decoded"));

        IOUtils.copyBytes(cis, fos, 1024 * 1024 * 5, false);
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fos);
    }


    public static void main(String[] args) throws Exception {
        DeCompressFile compressFile = new DeCompressFile();
        compressFile.defleateDeCompress();
    }
}
