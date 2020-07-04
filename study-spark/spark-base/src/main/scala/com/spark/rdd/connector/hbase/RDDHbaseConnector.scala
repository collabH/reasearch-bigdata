//package com.spark.rdd.connector.hbase
//
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.client.Result
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//
///**
//  * @fileName: RDDHbaseConnector.java
//  * @description: RDD连接HBASE
//  * @author: by echo huang
//  * @date: 2020-06-28 12:52
//  */
//object RDDHbaseConnector extends App {
//  override def main(args: Array[String]): Unit = {
//    val sc = new SparkContext("local", "hbaseConnector")
//    val hbaseConf = HBaseConfiguration.create()
//    hbaseConf.set(TableInputFormat.INPUT_TABLE, "student")
//    //读取hbase数据
//    val hbaseValue: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable],
//      classOf[Result])
//    hbaseValue.foreach {
//      case (k, v) =>
//        println(k)
//        println(v.rawCells())
//    }
//
//  }
//}
