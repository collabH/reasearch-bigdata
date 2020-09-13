package com.research.start.collection

import scala.collection.mutable.ListBuffer

/**
 * @fileName: ListApp.java
 * @description: list入口
 * @author: by echo huang
 * @date: 2020-03-12 18:03
 */
object ListApp extends App {
  //Nil相当于空的集合
  val list = List(1, 2, 3, 4, 5)

  //list是由head和tail构成
  println(list.head) //1
  println(list.tail) //2,3,4,5
  println(list(1)) //2

  //head::tail 在List前添加a

  val list1: List[Int] = 1 :: Nil
  val list2 = 2 :: list1
  println(3 :: list2)

  // ::: 在List前添加list
  println(list2.:::(List(4)))

  // filter exists
  println(list2.filter(data => data > 1))
  println(list2.exists(data => data == 2 && data == 3))
  // drop
  println(list2.drop(1))

  // foldLeft  类似foreach,可以给一个zeroValue 从左边开始计算
  println(list2.foldLeft(1)((total, data) => {
    total + data
  }))

  // 可变List
  val listBuffer: ListBuffer[Int] = ListBuffer(1, 2, 3, 4, 5, 6)

  listBuffer += 7

  listBuffer -= 2
  listBuffer -= 7

  println(listBuffer)

  println(listBuffer(1))


  def sum(nums: Int*): Int = {
    if (nums.isEmpty) {
      0
    } else {
      //将Seq转换为可变参数
      nums.head + sum(nums.tail: _*)
    }
  }

  print(sum(1, 2, 3, 4, 5, 6, 7, 8, 9, 0))

}
