package com.spark.rdd.connector.jdbc

import java.sql.DriverManager

import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

/**
  * @fileName: RDDJdbc.java
  * @description: rdd连接mysql
  * @author: by echo huang
  * @date: 2020-06-27 23:50
  */
object RDDJdbc extends App {


  override def main(args: Array[String]): Unit = {
    val url = "jdbc:mysql://localhost:3306/spark_study"
    val username = "root"
    val password = "root"

    val sc = new SparkContext("local", "mysql")
    //读取mysql
    val mysqlRDD = new JdbcRDD(sc, () => DriverManager.getConnection(url, username, password),
      "select id,name from user where id>=? and id <=?", 3, 6, 2,
      rs => {
        println(rs.getInt(1))
        println(rs.getString(2))
      })
    val connection = DriverManager.getConnection(url, username, password)
    val saveRDD = sc.makeRDD(List((17, "hhh"), (18, "zzz")), 2)
    saveRDD.collect()
      .foreach(data => {
        val statement = connection.prepareStatement("insert into user(id,name) values(?,?)")
        statement.setInt(1, data._1)
        statement.setString(2, data._2)
        statement.execute()
        statement.close()
        //            connection.close()
      })
    //解决序列化问题
    //    saveRDD.foreachPartition(datas => {
    //      val connection = DriverManager.getConnection(url, username, password)
    //      for (elem <- datas) {
    //        val statement = connection.prepareStatement("insert into user(id,name) values(?,?)")
    //        statement.setInt(1, elem._1)
    //        statement.setString(2, elem._2)
    //        statement.execute()
    //        statement.close()
    //      }
    //    })
  }
}
