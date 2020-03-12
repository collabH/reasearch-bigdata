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
    * @param grade
    */
  def judgeGrade(grade: String): Unit = {
     grade match {
       case "A" =>println("best")
       case "B" =>println("good")
       case "C" =>println("just so so")
       case _ =>println("you need work hard")
     }
  }

  judgeGrade("A")
  judgeGrade("B")
  judgeGrade("D")
}
