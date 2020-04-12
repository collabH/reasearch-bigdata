package org.hadoop.parquet.mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * @fileName: TextToParquetWithAvro.java
 * @description: TextToParquetWithAvro.java类说明
 * @author: by echo huang
 * @date: 2020-04-05 18:33
 */
public class TextToParquetWithAvro extends Configured implements Tool {
    private static final Schema SCHEMA = new Schema.Parser().parse("{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"User\",\n" +
            "  \"fields\": [\n" +
            "      {\"name\": \"name\", \"type\": \"string\"},\n" +
            "      {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n" +
            "      {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]},\n" +
            "      {\"name\": \"desc\", \"type\": [\"null\",\"string\"],\"default\":null}\n" +
            "  ]\n" +
            " }");


    public static class TextToParquetMapper extends Mapper<LongWritable, Text, Void, GenericRecord> {
        private GenericRecord record = new GenericData.Record(SCHEMA);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            record.put("name", "huangsm");
            record.put("favorite_number", 17);
            record.put("favorite_color", "red");
            record.put("desc", "红色");
            context.write(null, record);
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "parquet2avro");
        String output = "/user/parquet/avro2parquet.parquet";
        FileSystem fs = FileSystem.get(URI.create(output), getConf());
        if (fs.exists(new Path(output))) {
            fs.delete(new Path(output), true);
        }
        job.setJarByClass(getClass());

        String inputPath = "hdfs://localhost:8020/user/cache/user.parquet";
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(TextToParquetMapper.class);
        //设置0个reduce task
        job.setNumReduceTasks(0);

        //设置输出格式
        job.setOutputFormatClass(AvroParquetOutputFormat.class);

        //设置输出模式
        AvroParquetOutputFormat.setSchema(job, SCHEMA);

        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(GenericRecord.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TextToParquetWithAvro(), args);
        System.exit(exitCode);
    }
}
