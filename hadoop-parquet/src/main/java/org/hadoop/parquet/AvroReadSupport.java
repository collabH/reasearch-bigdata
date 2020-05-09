package org.hadoop.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;

/**
 * @fileName: AvroReadSupport.java
 * @description: 利用投影模式读取几列
 * @author: by echo huang
 * @date: 2020-04-05 18:22
 */
public class AvroReadSupport {
    public static void main(String[] args) throws IOException {
        ParquetReader<GenericRecord> reader = AvroParquetReader.builder(new org.apache.parquet.avro.AvroReadSupport<GenericRecord>(),
                new Path("hdfs://localhost:8020/user/cache/user.parquet")).build();
        GenericRecord read = reader.read();
        System.out.println(read);
    }
}
