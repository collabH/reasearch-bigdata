package com.research.traitdemo

import java.util.{Calendar, Date}

/**
 * @fileName: ImplicitDemo.scala
 * @description: ImplicitDemo.scala类说明
 * @author: by echo huang
 * @date: 2020/9/13 4:18 下午
 */
class ImplicitDemo(number: Int) {
  def days(when: String): Date = {
    var date = Calendar.getInstance()
    when match {
      case ImplicitDemo.ago => date.add(Calendar.DAY_OF_MONTH, -number)
      case ImplicitDemo.from_now => date.add(Calendar.DAY_OF_MONTH, number)
      case _ => date
    }
    date.getTime()
  }
}

class ImplicitDemo1(number: Long) {
  def days(when: String): Date = {
    var date = Calendar.getInstance()
    when match {
      case ImplicitDemo.ago => date.add(Calendar.DAY_OF_MONTH, -number.toInt)
      case ImplicitDemo.from_now => date.add(Calendar.DAY_OF_MONTH, number.toInt)
      case _ => date
    }
    date.getTime()
  }
}

object ImplicitDemo {
  val ago = "ago"
  val from_now = "from_now"

  implicit def convertInt2DateHelper(number: Int) = new ImplicitDemo(number)


  implicit def converLong2DataHelper(number: Long) = new ImplicitDemo1(number)

  def main(args: Array[String]): Unit = {
    2 days ago

    2L days ago
  }
}
