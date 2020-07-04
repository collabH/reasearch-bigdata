package com.spark.sql.udaf

import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}

/**
  * @fileName: SparkUDAF.java
  * @description: UDAF 用户自定义聚合函数
  * @author: by echo huang
  * @date: 2020-06-30 21:09
  */
object SparkUDAF extends App {

  override def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("udaf")
      .getOrCreate()
    //弱类型
    val udafFunction = new MyAgeFunction
    //注册函数
    spark.udf.register("avgAge", udafFunction)
    val df = spark.read.json("logs/user.json")
    df.show()
    df.createTempView("user")
    spark.sql("select avgAge(age) from user").show()


    //强类型聚合函数
    val ageClassFunction = new MyAgeClassFunction
    //将聚合函数转换为查询的列
    val avgClassAge = ageClassFunction.toColumn.name("avgClassAge")

    import spark.implicits._
    //需要dataSet,从文件中读取数值类型，spark会将其转换为bigint所以这边要定义为bigint
    val ds = df.as[UserBean]
    //通过dsl来查询
    ds.select(avgClassAge)
      .show()

    spark.close()
  }

}

/**
  * 自定义UDAF 求年龄的平均值
  * 1.继承UserDefinedAggregateFunction
  */
class MyAgeFunction extends UserDefinedAggregateFunction {
  /**
    * 数据数据结构
    *
    * @return
    */
  override def inputSchema: StructType = {
    new StructType()
      .add("age", dataType = IntegerType)
  }


  /**
    * 计算时的数据结构，缓存的数据结构
    *
    * @return
    */
  override def bufferSchema: StructType = {
    new StructType()
      .add("sum", IntegerType)
      .add("count", IntegerType)
  }

  /**
    * 函数返回的数据类型
    *
    * @return
    */
  override def dataType: DataType = {
    DoubleType
  }

  /**
    * 函数是否稳定，相同的入参返回值是否相同
    *
    * @return
    */
  override def deterministic: Boolean = {
    true
  }

  /**
    * 计算之前缓冲区的初始化
    *
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //定义第一个缓存的结构
    buffer(0) = 0
    //定义第二个缓存的结构
    buffer(1) = 0
  }

  /**
    * 根据查询结果age更新缓存区数据
    *
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //sum age
    buffer(0) = buffer.getInt(0) + input.getInt(0)
    //count
    buffer(1) = buffer.getInt(1) + 1
  }

  /**
    * 多节点缓存区合并
    *
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //sum
    buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
    //count
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }

  /**
    * 计算逻辑
    *
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = {
    buffer.getInt(0).toDouble / buffer.getInt(1)
  }
}

case class UserBean(var name: String, var age: BigInt)

case class AvgBuffer(var sum: BigInt, var count: BigInt)

/**
  * 自定义UDAF（强类型） 求年龄的平均值
  * 1.继承UserDefinedAggregateFunction
  */
class MyAgeClassFunction extends Aggregator[UserBean, AvgBuffer, Double] {
  /**
    * 初始化缓存区 零值
    *
    * @return
    */
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  /**
    * 查询出来的数据和当前缓冲区做聚合
    *
    * @param b
    * @param a
    * @return
    */
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  /**
    * 数据合并
    *
    * @param b1
    * @param b2
    * @return
    */
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count

    b1
  }

  /**
    * 完成计算
    *
    * @param reduction
    * @return
    */
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count.toDouble
  }

  /**
    * buffer编解码
    *
    * @return
    */
  override def bufferEncoder: Encoder[AvgBuffer] = {
    Encoders.product[AvgBuffer]
  }

  /**
    * 输出值编解码
    *
    * @return
    */
  override def outputEncoder: Encoder[Double] = {
    Encoders.scalaDouble
  }
}