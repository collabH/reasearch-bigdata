import org.apache.spark.{SparkConf, SparkContext}

/**
  * @fileName: WordCount.java
  * @description: WordCount.java类说明
  * @author: by echo huang
  * @date: 2020-06-25 22:44
  */
object WordCount extends App {
  override def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
//    conf.set("HADOOP_USER_NAME", "hadoop")
    val sc = new SparkContext("local", "wordCount", conf)
    val wordRdds = sc.textFile("hdfs://192.168.6.35:9000/user/hive/warehouse/forchange_prod.db/dim_date_t")
    wordRdds.foreach(println)
    //    val tuples = wordRdds.flatMap(_.split(","))
    //      .map((_, 1))
    //      .reduceByKey(_ + _)
    //      .collect()
    //    for (elem <- tuples) {
    //      print(elem)
    //    }
  }
}
