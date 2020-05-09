package com.research.hadoop.join.reduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * @fileName: JoinReduceDriver.java
 * @description: JoinReduceDriver.java类说明
 * @author: by echo huang
 * @date: 2020-03-27 11:49
 */
public class JoinReduceDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "joinReduce");
        job.setJarByClass(getClass());
        String output = "/user/join/reduce";
        FileSystem fs = FileSystem.get(URI.create(output), getConf());
        if (fs.exists(new Path(output))) {
            fs.delete(new Path(output), true);
        }
        Path stationInputPath = new Path("/user/station.txt");
        Path record = new Path("/user/record.txt");

        MultipleInputs.addInputPath(job, stationInputPath, TextInputFormat.class, StationMapper.class);
        MultipleInputs.addInputPath(job, record, TextInputFormat.class, RecordMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setPartitionerClass(JoinRecordWithStationName.class);
        job.setCombinerKeyGroupingComparatorClass(TextPair.FirstCompartor.class);

        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        return job.waitForCompletion(true) ? 1 : 0;
    }

    public static void main(String[] args) throws Exception {
        int exit = ToolRunner.run(new JoinReduceDriver(), args);
        System.exit(exit);
    }
}
