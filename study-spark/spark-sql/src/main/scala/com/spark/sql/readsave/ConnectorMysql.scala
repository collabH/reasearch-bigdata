package com.spark.sql.readsave

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @fileName: ConnectorMysql.java
  * @description: ConnectorMysql.java类说明
  * @author: by echo huang
  * @date: 2020-06-30 23:46
  */
object ConnectorMysql extends App {
  override def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("mysql")
      .master("local[*]")
      .getOrCreate()
    //第一种参数传递方式
    val mysqlDF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/sandbox-monitor")
      .option("dbtable", "datasource_config")
      .option("user", "root")
      .option("password", "root")
      .load()
    mysqlDF.show()

    //通过系统属性传递参数
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "root")
    var df2 = spark.read.format("jdbc")
      .jdbc("jdbc:mysql://localhost:3306/sandbox-monitor", "user_domain_config", properties)
    df2.show()
    df2.createTempView("temp")
    df2 = spark.sql("select domain from temp")
    df2.show()
    //保存数据
    df2.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/sandbox-monitor", "user_domain_config", properties)
    spark.close()

  }
}
