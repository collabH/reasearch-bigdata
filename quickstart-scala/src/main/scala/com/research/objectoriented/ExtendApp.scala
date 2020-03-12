package com.research.objectoriented

/**
  * @fileName: ExtendApp.java
  * @description: 继承入口
  * @author: by echo huang
  * @date: 2020-03-12 16:12
  */
object ExtendApp {

  def main(args: Array[String]): Unit = {
    //new子类回先调用父类构造方法，类比java
    val tom = new Dog("tom", "male", "s")
    println(tom.toString)
    println(tom.eat)
  }
}

class Animal(val name: String, val gender: String) {

  println("animal:", this.name, this.gender)

  def eat = {
    println("吃")
  }
}

 class Dog(override val name: String, override val gender: String, eat: String) extends  Animal(name, gender) {
  println("dog:", this.name, this.gender, this.eat)
  override def toString: String = "Dog is overrider"
}




