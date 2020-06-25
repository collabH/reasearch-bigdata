import org.apache.spark.SparkContext

/**
  * @fileName: WordCount.java
  * @description: WordCount.java类说明
  * @author: by echo huang
  * @date: 2020-06-25 22:44
  */
class WordCount {


}
object WordCount extends App {
  override def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","wordCount")
    val wordRdds = sc.textFile("file:///Users/babywang/Documents/reserch/studySummary/bigdata/spark/spark.txt")
    val tuples = wordRdds.flatMap(_.split(","))
      .map((_, 1))
      .reduceByKey(_ + _)
      .collect()
    for (elem <- tuples) {
      print(elem)
    }
  }
}
