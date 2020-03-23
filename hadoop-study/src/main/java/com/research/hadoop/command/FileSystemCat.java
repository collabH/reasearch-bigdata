package com.research.hadoop.command;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * @fileName: FileSystemCat.java
 * @description: FileSystemCat.java类说明
 * @author: by echo huang
 * @date: 2020-03-17 15:43
 */
public class FileSystemCat {
    public static void main(String[] args) throws IOException {
        String path = "hdfs://localhost:8020/user/text.txt";
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(path), configuration);
        FSDataInputStream in = fs.open(new Path(path));
        IOUtils.copy(in, System.out);
        in.seek(1);
        IOUtils.copy(in, System.out);

    }
}
