package org.hadoop.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;

/**
 * @fileName: Main.java
 * @description: Main.java类说明
 * @author: by echo huang
 * @date: 2020-04-02 21:50
 */
public class Main {
    public static void main(String[] args) throws IOException {
//        quickStart();
        readRarquetFile("hdfs://localhost:8020/user/cache/data.parquet");
    }

    public static void readRarquetFile(String path) throws IOException {
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader<Group> reader = ParquetReader.builder(readSupport, new Path(path)).build();
        //读取下一个message
        Group read = reader.read();
        System.out.println(read);
        System.out.println(read.getString("left",0));
    }

    /**
     * 创建parquet文件并想文件写入一个message
     */
    public static void writerParquetFile(String path) throws IOException {
        //定义Parquet模式，message所包含的值为逻辑类型UTF8，Group为我们提供它与String自然转换
        MessageType schema = MessageTypeParser.parseMessageType("message Pari{\n" +
                " required binary left(UTF8);\n" +
                " required binary right(UTF8);\n" +
                "}");
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        Group group = groupFactory.newGroup()
                .append("left", "L")
                .append("right", "R");

        Configuration conf = new Configuration();
        Path dirPath = new Path(path);
        GroupWriteSupport writerSupport = new GroupWriteSupport();
        GroupWriteSupport.setSchema(schema, conf);
        ParquetWriter<Group> writer = new ParquetWriter<Group>(dirPath, writerSupport,
                ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                ParquetProperties.WriterVersion.PARQUET_1_0, conf);
        writer.write(group);
        writer.close();
    }

    /**
     * 定义一个Pari格式为逻辑类型UTF8
     */
    public static void quickStart() {
        //定义Parquet模式，message所包含的值为逻辑类型UTF8，Group为我们提供它与String自然转换
        MessageType schema = MessageTypeParser.parseMessageType("message Pari{\n" +
                " required binary left(UTF8);\n" +
                " required binary right(UTF8);\n" +
                "}");

        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        Group group = groupFactory.newGroup()
                .append("left", "L")
                .append("right", "R");

        System.out.println(group);
    }
}
