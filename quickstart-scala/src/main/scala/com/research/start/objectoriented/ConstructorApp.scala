package com.research.start.objectoriented

/**
  * @fileName: ConstructorApp.java
  * @description: 构造器入口
  * @author: by echo huang
  * @date: 2020-03-12 15:58
  */
object ConstructorApp {

  def main(args: Array[String]): Unit = {
    val person = new ConstructorPerson("zairian", 10)


    val hsmPerson = new ConstructorPerson("hsm", 21, "男")
    println(hsmPerson)
  }
}


//主构造器
class ConstructorPerson(val name: String, val age: Int) {
  println(name, age)

  var sex: String = _

  //附属构造器
  def this(name: String, age: Int, sex: String) {
    //附属构造器的第一行代码必须调用只构造器或者其他附属构造器
    this(name, age)
    this.sex = sex
  }

  override def toString: String = this.name + ":" + this.age + ":" + this.sex
}

