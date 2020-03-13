package com.research.scalabook.grammar

import java.util.concurrent.{Executor, FutureTask, TimeUnit}

import scala.collection.immutable.NumericRange
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

/**
  * @fileName: GrammarApp.java
  * @description: 语法学习
  * @author: by echo huang
  * @date: 2020-03-13 16:33
  */
class GrammarApp {

  /**
    * 分号
    *
    * @param s
    */
  // 末尾的等号表明下一行还有未结束的代码。
  def equalsign(s: String) =
    println("equalsign: " + s)

  // 末尾的花括号表明下一行还有未结束的代码。
  def equalsign2(s: String) = {
    println("equalsign2: " + s)
  }

  // 末尾的逗号、句号和操作符都可以表明，下一行还有未结束的代码。
  def commas(s1: String,
             s2: String) = Console.
    println("comma: " + s1 +
      ", " + s2)

  /**
    * 变量声明
    */

  var name = "zhangsan"
  val age = 10

  class Person(var name: String, val age: Int) {

  }

  val person = new Person("zhang", 10)

  //  person.age=10 报错不可变


  /**
    * Range
    */
  object RangeTest {
    //
    1 to 10

    //Int类型的Range，不包括区间上限，步长1(1到9)
    1 until 10

    //Int类型的Range，包括区间上限，步长3
    1 to 10 by 3


    //Int类型的递减Range，包括区间上限，步长-3
    10 to 1 by -3


    //Long类型
    val longs: NumericRange[Long] = 1L to 10L by 3

    //Char类型
    'a' to 'z' by 1
  }

}

/**
  * 偏函数学习
  */
object PartialFunctionTest extends App {
  val pf1: PartialFunction[Any, String] = {
    case s: String => s"wc $s"
  }

  val pf2: PartialFunction[Any, String] = {
    case i: Int => i.toBinaryString
  }

  val pf = pf1 orElse pf2

  def tryPf(x: Any, f: PartialFunction[Any, String]) = {
    try {
      f.apply(x).toString
    } catch {
      case _: MatchError => "ERROR!"
    }
  }

  println(tryPf("123", pf2))

  def testDefinedAt(x: Any, f: PartialFunction[Any, String]) = {
    //判断输入类型偏函数case是否匹配
    if (f.isDefinedAt(x)) {
      throw new RuntimeException("异常数据")
    } else {
      f.apply(x)
    }
  }

  //testDefinedAt(10, pf2)

  println(pf1.apply("xx").->(10))
}

/**
  * 方法默认值和命名参数列表
  */
object defaultMethod extends App {


  //方法默认值
  case class PointC(x: Double = 0.0)(a: Double = 0.0) {
    def shift(deltax: Double = 0.0)(z: Double) = {
      //case类自动创建的，允许你在创建case类的新实例时，只给出原对象不同部分的参数,相当于copy当前对象
      copy _
    }
  }

  val c = PointC(1.0)(2.0)
  //命名参数列表
  val c1 = c.copy(x = 1.0)(2.0).shift(deltax = 10)(z = 1.0)
  println(c.isInstanceOf[PointC])

}

object FutureTest extends App {
  def sleep(millis: Long) = {
    Thread.sleep(millis)
  }

  /**
    * 繁忙的工作处理
    */
  def doWork(index: Int) = {
    sleep((math.random * 1000).toLong)
    index
  }

  (1 to 5).foreach(index => {
    Future
    val futurn = new FutureTask[Int](() => {
      doWork(index)
    })
    println(futurn.get(10, TimeUnit.MILLISECONDS))
  })

  1.toString

  1 + 2.*(3)
}

object Implicits {
  implicit val global: ExecutionContextExecutor = {
    ExecutionContext.fromExecutor(null: Executor)
  }
}

object tset extends App {
  List(1, 2, 3, 4) foreach println

}

object YieldTest extends App {
  val list = List(1, 2, 3, 4)
   val ints: List[Int] = for (i <- list if i % 2 == 0) yield i
  println(ints)
}
