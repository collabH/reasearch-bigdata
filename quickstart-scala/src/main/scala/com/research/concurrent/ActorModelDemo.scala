package com.research.concurrent

/**
 * @fileName: ActorModelDemp.scala
 * @description: ActorModelDemp.scala类说明
 * @author: by echo huang
 * @date: 2020/9/13 6:18 下午
 */
class ActorModelDemo {
  // 求和
  def sum(lower: Int, upper: Int, number: Int) = {
    var sum = 0;
    for (elem <- lower to upper) {
      if (number % elem == 0) {
        sum += elem
      } else sum
    }
  }

  def isPerfectConcurrent(candidate: Int) = {
    val RANGE = 1000000
    val numberOfPartitions = (candidate.toDouble / RANGE).ceil.toInt


  }
}
