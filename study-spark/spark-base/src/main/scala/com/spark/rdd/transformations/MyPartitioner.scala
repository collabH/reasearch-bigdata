package com.spark.rdd.transformations

import org.apache.spark.Partitioner

/**
  * @fileName: MyPartitioner.java
  * @description: 自定义分区器
  * @author: by echo huang
  * @date: 2020-06-27 00:45
  */
class MyPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = {
    partitions
  }

  override def getPartition(key: Any): Int = {
    1
  }
}
