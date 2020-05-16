package com.spark.checkpoint;

import com.spark.factory.JavaStreamingFactroy;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @fileName: UseCheckpointing.java
 * @description: 使用checkpoint
 * @author: by echo huang
 * @date: 2020-04-25 17:15
 */
public class UseCheckpointing {
    public static void main(String[] args) throws Exception {
        JavaStreamingFactroy javaStreamingFactroy = new JavaStreamingFactroy() {

            @Override
            public JavaStreamingContext create() {
                SparkConf sparkConf = new SparkConf().setMaster("local[2]")
                        .setAppName("checkpoint");
                JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Duration.apply(1000));  // new context
                JavaDStream<String> lines = jssc.socketTextStream("localhost", 10086);     // create DStreams
//                jssc.checkpoint(checkpointDirectory);                       // set checkpoint directory
                return jssc;
            }
        };

        JavaStreamingContext.getOrCreate("test", javaStreamingFactroy);
        javaStreamingFactroy.call().start();
        javaStreamingFactroy.call().awaitTermination();

    }
}
