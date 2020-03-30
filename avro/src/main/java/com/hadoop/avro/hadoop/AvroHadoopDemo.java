package com.hadoop.avro.hadoop;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;

/**
 * @fileName: AvroHadoopDemo.java
 * @description: hadoop的avro
 * @author: by echo huang
 * @dat+e: 2020-03-30 16:45
 */
public class AvroHadoopDemo extends Configured implements Tool {

    private static Schema SCHEMA = null;

    static {
        try {
            SCHEMA = new Schema.Parser().parse(AvroHadoopDemo.class.getClassLoader().getResourceAsStream("temp.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class MaxMapper extends Mapper<LongWritable, Text, AvroKey<Integer>, AvroValue<GenericRecord>> {
        private GenericRecord record = new GenericData.Record(SCHEMA);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            Integer year = Integer.valueOf(tokens[0]);
            String temperature = tokens[1];
            String stationId = tokens[2];
            record.put("year", year);
            record.put("temperature", temperature);
            record.put("stationId", stationId);
            context.write(new AvroKey<>(year), new AvroValue<>(record));
        }
    }

    public static class MaxReduce extends Reducer<AvroKey<Integer>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> {
        @Override
        protected void reduce(AvroKey<Integer> key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {
            GenericRecord max = null;
            for (AvroValue<GenericRecord> value : values) {
                GenericRecord record = value.datum();
                if (max == null || (Integer) record.get("temperature") > (Integer) max.get("temperature")) {
                    max = newWeatherRecord(record);
                }
            }
            context.write(new AvroKey<>(max), NullWritable.get());
        }

        private GenericRecord newWeatherRecord(GenericRecord value) {
            GenericRecord record = new GenericData.Record(SCHEMA);
            record.put("year", value.get("year"));
            record.put("temperature", value.get("temperature"));
            record.put("stationId", value.get("stationId"));
            return record;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "avro-max-air");
        //添加第三方jar包依赖
        job.addArchiveToClassPath(new Path("/cache/avro-mapred-1.9.2.jar"));
        job.addArchiveToClassPath(new Path("/cache/avro-1.9.2.jar"));

        String outputPath = "/cache/max.txt";
        FileSystem fs = FileSystem.get(URI.create(outputPath), getConf());
        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath), true);
        }
        job.setJarByClass(getClass());
        job.setMapperClass(MaxMapper.class);
        job.setReducerClass(MaxReduce.class);


        FileInputFormat.addInputPath(job, new Path("/cache/air.txt"));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setMapOutputKeySchema(job, SCHEMA);
        AvroJob.setOutputKeySchema(job, SCHEMA);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        return job.waitForCompletion(true) ? 1 : 0;
    }

    public static void main(String[] args) throws Exception {
        int exit = ToolRunner.run(new AvroHadoopDemo(), args);
        System.exit(exit);
    }
}
