package com.research.scalabook

/**
  * @fileName: ScalaBookApp.java
  * @description: ScalaBookApp.java类说明
  * @author: by echo huang
  * @date: 2020-03-13 15:19
  */
object ScalaBookApp extends App {

  Array(1, 2, 3, 4, 5).map(_ + 1)
    .foreach(k => {
      println()
      printf("%d", k)
    })
}
