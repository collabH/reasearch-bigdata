package com.research.patternmatch

import scala.util.Random

/**
  * @fileName: MatchApp.java
  * @description: MatchApp.java类说明
  * @author: by echo huang
  * @date: 2020-03-12 18:50
  */
object MatchApp extends App {


  val names = Array("longfuyou", "tom", "hsm")


  val name: String = names(Random.nextInt(2))


  val value: ThreadLocal[String] = ThreadLocal.withInitial(() => new String())

  value.set("hello")
  println(value.get())

  name match {
    case "longfuyou" => println("这是一个憨逼")
    case "tom" => println("这也是一个憨逼")
    case "hsm" => println("这是一个帅比")
  }


  /**
    * 分数
    *
    * @param grade
    */
  def judgeGrade(grade: String): Unit = {
    grade match {
      case "A" => println("best")
      case "B" => println("good")
      case "C" => println("just so so")
      case _ => println("you need work hard")
    }
  }

  def judgeGradeAndName(name: String, grade: String): Unit = {
    grade match {
      case "A" => println("best")
      case "B" => println("good")
      case "C" => println("just so so")
      case _ if name.equals("longfuyou") => println(name + "你很好，但是...")
      case _ => println("you need work hard")
    }
  }

  judgeGrade("A")
  judgeGrade("B")
  judgeGrade("D")

  judgeGradeAndName("longfuyou", "D")

  //数组模式匹配

  def greeting(array: Array[String]) = {
    array match {
      case Array(x, y) => println(array.mkString(" "))
      case Array("zhangsan") => println("hi zhangsan")
      case Array("zhangsan", _*) => println("hi zhangsan and other")
      case _ => println("xxx")
    }
  }

  greeting(Array("zhangsan", "Dsad", "Dsad"))


  //集合list匹配


  def listMatch(list: List[String]): Unit = {
    list match {
      case "zhangsan" :: Nil => println("zhangsan")
      case _ :: _ :: Nil => println("俩个元素匹配" + list)
      case List("zhangsan", _ *) => println(list)
      case _ => println("other")
    }
  }

  listMatch(List("zhangsan"))

  listMatch(List("zhangsan", "hh"))


  //类型匹配
  def matchType(any: Any) = {
    any match {
      case i: Int => println("int")
      case l: Long => println("long")
      case m: Map[_, _] => m.foreach(println)
    }
  }

  matchType(1l)

  matchType(Map(1 -> 2))


  //异常处理
  def matchException(exception: Exception) = {
    exception match {
      case e:RuntimeException=>throw e
      case t:NullPointerException =>throw t
      case e:IndexOutOfBoundsException => throw e
    }
  }

  //matchException(new RuntimeException("我是运行时异常"))
  matchException(new NullPointerException("我是空指针异常"))
}
