package com.research.start.objectoriented

/**
  * @fileName: ApplyApp.java
  * @description: ApplyApp.java类说明
  * @author: by echo huang
  * @date: 2020-03-12 16:52
  */
object ApplyApp {

  def main(args: Array[String]): Unit = {

    /* for (i <- 1 to 10) {
       //object不需要new对象直接添加即可
       ApplyTest.incr
     }

     //object本身就是单例对象
     println(ApplyTest.count)*/

    val o = ApplyTest() //===>Object.apply方法

    println("~~~")
    val c = new ApplyTest()
    println(c)
    c()
    //类名()===>Object.apply方法
    //对象()===>Class.apply方法

  }
}

/**
  * 伴生类和伴生对象
  * 如果又一个class，还有一个与class同名的object
  * 那么就称这个class是object的伴生类，object就是class的伴生对象
  */
class ApplyTest {
  def apply() = println("Class ApplyTest apply...")
}

object ApplyTest {
  println("enter")
  var count = 0

  def incr = {
    count += 1
  }

  //最佳实践:在Object的apply方法中去new Class
  def apply() = {
    println("Object ApplyTest apply...")
    //在object上new一个class
    new ApplyTest
  }

  println("leave")
}