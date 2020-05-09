package org.hadoop.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.IOException;
import java.io.InputStream;

/**
 * @fileName: AvroParquet.java
 * @description: AvroParquet.java类说明
 * @author: by echo huang
 * @date: 2020-04-02 23:34
 */
public class AvroParquet {
    public static void main(String[] args) throws IOException {
//        new AvroParquet().avroParquet();
        AvroParquet avroParquet = new AvroParquet();
        avroParquet.read();
    }

    public void read() throws IOException {
        GroupReadSupport groupReadSupport = new GroupReadSupport();
        ParquetReader<Group> build = ParquetReader.builder(groupReadSupport, new Path("hdfs://localhost:8020/user/cache/user.parquet"))
                .build();
        System.out.println(build.read());
    }

    public void avroParquet() throws IOException {
        Schema.Parser parser = new Schema.Parser();
        InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream("user1.avsc");
        System.out.println(resourceAsStream);
        Schema parse = parser.parse(resourceAsStream);
        GenericData.Record datum = new GenericData.Record(parse);
        datum.put("name", "hsm");
        datum.put("favorite_number", 11);
        datum.put("favorite_color", "red");

        Path path = new Path("hdfs://localhost:8020/user/cache/user.parquet");
        ParquetWriter<Object> writer = AvroParquetWriter.builder(path)
                .withSchema(parse)
                .build();
        writer.write(datum);
        writer.close();
    }
}
