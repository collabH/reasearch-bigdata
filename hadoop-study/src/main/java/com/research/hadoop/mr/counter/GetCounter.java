package com.research.hadoop.mr.counter;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @fileName: GetCounter.java
 * @description: GetCounter.java类说明
 * @author: by echo huang
 * @date: 2020-03-26 17:56
 */
public class GetCounter extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            return -1;
        }
        String jobID = args[0];
        Cluster cluster = new Cluster(getConf());
        Job job = cluster.getJob(JobID.forName(jobID));
        if (!job.isComplete()) {
            System.err.println("not complete");
            return -1;
        }
        System.out.println(job.getCounters().findCounter(JobCounter.NUM_UBER_SUBMAPS).getValue());
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int count = ToolRunner.run(new GetCounter(), args);
        System.exit(count);
    }
}
