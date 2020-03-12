package com.research.objectoriented

/**
  * @fileName: TraitApp.java
  * @description: TraitApp.java类说明
  * @author: by echo huang
  * @date: 2020-03-12 17:18
  */
object TraitApp {
  def main(args: Array[String]): Unit = {
    val person = new Person
    person.eat
    person.say
  }
}

trait Say {
  def say = println("say")
}

trait Eat {
  def eat = println("eat")
}

class Person extends Say with Eat {

}
