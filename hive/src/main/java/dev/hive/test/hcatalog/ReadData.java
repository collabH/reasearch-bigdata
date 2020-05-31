package dev.hive.test.hcatalog;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatBaseInputFormat;

import java.io.IOException;

/**
 * @fileName: ReadData.java
 * @description: ReadData.java类说明
 * @author: by echo huang
 * @date: 2020-05-31 14:55
 */
public class ReadData {
    public static void main(String[] args) throws IOException {
        HCatSchema tableSchema = HCatBaseInputFormat.getTableSchema(new Configuration());
        HCatFieldSchema name = tableSchema.get("name");
        HCatFieldSchema age = tableSchema.get("age");
        HCatBaseInputFormat.setOutputSchema(Job.getInstance(), new HCatSchema(Lists.newArrayList(name, age)));
    }
}
