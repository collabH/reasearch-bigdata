package com.hadoop.avro.javabean;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * @fileName: AvroSeriableAndDerSeriable.java
 * @description: AvroSeriableAndDerSeriable.java类说明
 * @author: by echo huang
 * @date: 2020-03-30 15:39
 */
public class AvroSeriableAndDerSeriable {

    private static User user = new User();
    private static User user1 = User.newBuilder()
            .setName("user1")
            .setFavoriteColor("red")
            .setFavoriteNumber(12).build();
    private static User user2 = new User("user2", 15, "black");

    static {
        user.setFavoriteColor("yellow");
        user.setName("user");
        user.setFavoriteNumber(10);
    }

    public static void main(String[] args) throws IOException {
//        seriableObject();
        dersearableArvo();
    }

    /**
     * 序列化对象到文件
     */
    private static void seriableObject() throws IOException {
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>();

        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer);

        dataFileWriter.create(user.getSchema(), new File(AvroSeriableAndDerSeriable.class.getResource("/").getPath() + "user.arvo"));
        dataFileWriter.append(user);
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.close();
    }

    private static void dersearableArvo() throws IOException {
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(user2.getSchema());
        File file = FileUtils.getFile(AvroSeriableAndDerSeriable.class.getClassLoader().getResource("user.arvo").getPath());
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, reader);
        //支持随机读取类似于DataInputStream
        GenericRecord next = dataFileReader.next();
        System.out.println(next);
    }
}
