package com.research.traitdemo

/**
 * @fileName: Friend.scala
 * @description: Friend.scala类说明
 * @author: by echo huang
 * @date: 2020/9/13 3:56 下午
 */
trait Friend {

  val name: String

  def listen() = println(s"Your friend $name is listening")
}

class Human(val name: String) extends Friend {
}

class Woman(override val name: String) extends Human(name) {
}

object App {
  def main(args: Array[String]): Unit = {
    val cat = new Cat("cat") with Friend
    cat listen
  }
}

class Cat(val name: String) {

}

