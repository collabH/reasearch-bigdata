package com.research.hadoop.mapreduce.input;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @fileName: MutilInputTest.java
 * @description: MutilInputTest.java类说明
 * @author: by echo huang
 * @date: 2020-03-26 15:17
 */
public class MutilInputTest extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
//        ToolRunner.run(new MutilInputTest(), args);
        System.out.println(!(true||false));
    }

    @Override
    public int run(String[] args) throws Exception {
        MultipleInputs.addInputPath(Job.getInstance(), new Path("xxx"), TextInputFormat.class);
        return 1;
    }
}
