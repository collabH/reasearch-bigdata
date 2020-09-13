package com.research

/**
 * @fileName: ForDemo.scala
 * @description: ForDemo.scala类说明
 * @author: by echo huang
 * @date: 2020/9/13 5:32 下午
 */
object ForDemo extends App {
  // 1 2 3
  for (i <- 1 to 3) {
    println(i)
  }
  // 1 2
  for (elem <- 1 until 3) {
    println(elem)
  }
  // for filter
  for (elem <- 1 to 10; if elem % 2 == 0) {
    println(elem)
  }

  for {
    i <- 1 to 3
    if i > 2
  } {
    println(i)
  }
}
