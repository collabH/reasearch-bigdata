package com.hadoop.avro.javabean;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

/**
 * @fileName: JavaBeanApp.java
 * @description: JavaBeanApp.java类说明
 * @author: by echo huang
 * @date: 2020-03-30 09:53
 */
public class JavaBeanApp {


    public static void main(String[] args) throws IOException {
//        seariable2Disk();
        dataToDiskAvsc();
    }

    private static void readDataFromHadoop(){

    }

    private static void dataToDiskAvsc() throws IOException {
        User user = new User();
        user.setFavoriteNumber(11);
        user.setFavoriteColor("red");
        user.setName("hsm");
        File file = new File("data.avro");
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(user.getSchema());


        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer);
        dataFileWriter.create(user.getSchema(), file);
        dataFileWriter.append(user);
        dataFileWriter.close();

    }

    /**
     * 使用特定API和maven avro插件
     */
    private static void seariable2Disk() {
        User user = new User();
        user.setFavoriteNumber(11);
        user.setFavoriteColor("red");
        user.setName("hsm");

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            DatumWriter<User> writer = new SpecificDatumWriter<>(User.class);
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(user, encoder);
            encoder.flush();

            DatumReader<User> reader = new SpecificDatumReader<>(User.class);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
            User result = reader.read(null, decoder);
            System.out.println(result);


        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
