package com.research

import com.research.DayOfWeek.SUNDAY

/**
 * @fileName: CaseDemo.scala
 * @description: CaseDemo.scala类说明
 * @author: by echo huang
 * @date: 2020/9/13 5:49 下午
 */
object CaseDemo extends App {
  // 字符串匹配
  def stringCase(word: String) = {
    word match {
      case "a" =>
        println(word)
    }
  }

  stringCase("a")

  // 匹配通配符
  def enumCase(word: String) = {
    DayOfWeek.withName(word) match {
      case SUNDAY => {
        println(SUNDAY.toString)
      }
    }
  }

  enumCase("Sunday")

  // 匹配元组
  def tupleCase(input: Any) = {
    input match {
      case (a, b) => println(a, b)
      case "done" => println("done...")
      case _ => None
    }
  }

  // 匹配list
  def listCase(list: List[String]) = {
    list match {
      case List("a") => println(list)
      case ::(head, tail) => println(head, tail)
    }
  }

  listCase(List("a"))
  listCase(List("a", "c"))

  tupleCase((1, 2))
  tupleCase("done")
  println(tupleCase(1))

  // case表达式的模式
  class Sample {
    val max = 100
    val MIN = 0

    def process(input: Int) {
      input match {
        case this.max => println("You matched max")
        case MIN => println("You matched min")
        case _ => println("Unmatched")
      }
    }
  }

  new Sample().process(100)
  new Sample().process(0)
  new Sample().process(10)

  // 使用case类模式匹配
  def caseClassCase(trade: Trade) = {
    trade match {
      case Buy() => println("buy")
      case Sell() => println("sell")
      case _ => println("nothing")
    }
  }

  caseClassCase(Buy())
  caseClassCase(Sell())
}

object DayOfWeek extends Enumeration {
  val SUNDAY = Value("Sunday")
  val MONDAY = Value("Monday")
  val TUESDAY = Value("Tuesday")
  val WEDNESDAY = Value("Wednesday")
  val THURSDAY = Value("Thursday")
  val FRIDAY = Value("Friday")
  val SATURDAY = Value("Saturday")
}

// 使用case类进行模式匹配
abstract class Trade()

case class Sell() extends Trade

case class Buy() extends Trade
