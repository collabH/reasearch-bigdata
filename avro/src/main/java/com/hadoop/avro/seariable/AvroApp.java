package com.hadoop.avro.seariable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import javax.xml.ws.EndpointReference;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @fileName: AvroApp.java
 * @description: 读写arvo数据
 * @author: by echo huang
 * @date: 2020-03-29 18:46
 */
public class AvroApp {



    /**
     * 读取avro数据
     *
     * @param inputStream
     */
    private static Schema readAvro(InputStream inputStream) throws IOException {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(inputStream);
        System.out.println(schema);
        return schema;
    }

    /**
     * 设置数据
     *
     * @param schema
     */
    private static GenericRecord setData(Schema schema) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("name", "wy");
        record.put("favorite_number", 10);
        record.put("favorite_color", "黑色");
        System.out.println(record);
        return record;
    }

    /**
     * 序列化数据
     *
     * @param record
     */
    private static void seariableData(GenericRecord record,Schema schema) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
            writer.write(record, encoder);
            encoder.flush();
            //从流中读取数据
            DatumReader<GenericRecord> reader=new GenericDatumReader<>(schema);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(baos.toByteArray(), null);
            GenericRecord genericRecord = reader.read(null, decoder);
            System.out.println(genericRecord.get("name"));
            System.out.println(genericRecord.get("favorite_number"));
            System.out.println(genericRecord.get("favorite_color"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        InputStream inputStream = ClassLoader.getSystemClassLoader().getResourceAsStream("user.avsc");
        Schema schema = readAvro(inputStream);
        //seariableData(setData(schema),schema);
    }
}
