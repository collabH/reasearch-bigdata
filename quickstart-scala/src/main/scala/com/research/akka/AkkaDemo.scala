package com.research.akka

import akka.actor.{ActorRef, ActorSystem, Props}

/**
 * @fileName: AkkaDemo.scala
 * @description: AkkaDemo.scala类说明
 * @author: by echo huang
 * @date: 2021/4/4 12:23 上午
 */
object AkkaDemo {
  def main(args: Array[String]): Unit = {
    // actor1创建的ActorSystem名称
    val actor1: ActorSystem = ActorSystem.create("actor1")
    val actor2: ActorSystem = ActorSystem.create("actor2")

    // 获取actorRef
    val hello: ActorRef = actor1.actorOf(Props.create(ActorRef.getClass), "hello")

    // 不会阻塞后续代码运行,第二个参数表述无发送者
    hello.tell("hello actor", ActorRef.noSender)


  }
}