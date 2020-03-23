package com.research.start.highfunction

/**
  * @fileName: StringHighOperator.java
  * @description: 字符串的高阶操作
  * @author: by echo huang
  * @date: 2020-03-13 00:07
  */
object StringHighOperator extends App {
  //基础字符串操作
  val s = "hello"
  val name = "hsm"
  println(s + name)

  //高阶字符串操作
  //1.字符串插值
  println(s"Hello:$name")

  val  team="Forchange"

  println(s"Hello:$name,welcome to $team")


  //2.多行字符串
  val b=
    """这是一个多行字符串
      |hello
      |world
      |hsm
    """.stripMargin
  println(b)
}
