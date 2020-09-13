package com.research.scalabook.concurrent

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

/**
 * @fileName: Shape.java
 * @description: Shape.java类说明
 * @author: by echo huang
 * @date: 2020-03-13 15:33
 */
case class Point(x: Double = 0.0, y: Double = 0.0)

abstract class Shape {

  /**
   * draw方法接受一个函数参数。每个图形对象都会将自己的字符格式传给函数f，
   * 由函数f执行绘制工作。
   */
  //String=>Unit代表函数f接受字符串参数输入并返回Unit类型，draw传递一个函数参数，unit与java中的void相似
  def draw(f: String => Unit): Unit = f(s"draw: ${this.toString}")
}


case class Circle(center: Point, radius: Double) extends Shape

case class Rectangle(lowerLeft: Point, height: Double, width: Double)
  extends Shape

case class Triangle(point1: Point, point2: Point, point3: Point)
  extends Shape

object Message {

  object Exit

  object Finished

  case class Response(message: String)

}

/**
 * 包括 Akka 在内的大多数 actor 系统中，每一个 actor 都会有一个关联邮箱（mailbox）。
 * 关联邮箱中存储着大量消息，而这些消息只有经过 actor 处理后才会被提取。
 * Akka 确保了消息处理的顺序与接收顺序相同，而对于那些正在被处理的消息，
 * Akka 保证不会有其他线程抢占该消息。因此，使用 Akka 编写的消息处理代码天生具有线程安全的特性。
 *
 */
class ShapesDrawingActor extends Actor {

  //“嵌套导入 （nesting import），嵌套导入会限定这些值的作用域。”

  import Message._

  /**
   * 定义如何处理接受的消息
   *
   * @return
   */
  override def receive: Receive = {
    //偏函数 PartialFunction[Any,Unit] 接受Any返回Unit
    //继承了模式匹配和子类型多态
    case s: Shape => s.draw(str => println(s"ShapesDrawingActor:$str"))
      //def !(message : scala.Any)(implicit sender : akka.actor.ActorRef = { /* compiled code */ }) : scala.Unit
      //代表创建回复消息，并将该消息发送给了shape对象的发送方，Actor.sender函数返回了actor发送消息接受方的对象引用，而!方法则用于异步消息
      sender ! Response(s"ShapesDrawingActor: $s drawn")

    case Exit =>
      println(s"ShapesDrawingActor: exiting...")
      sender ! Finished
    case any =>
      val response: Response = Response(s"ERROR: Unknown message: $any")
      println(s"ShapesDrawingActor: $response")
      sender ! response
  }
}

private object Start


object ShapesDrawingDriver {
  def main(args: Array[String]): Unit = {
    val system: ActorSystem = ActorSystem("DrawingActorSystem", ConfigFactory.load())
    val drawer: ActorRef = system.actorOf(Props(new ShapesDrawingActor), "drawingActor")
    val driver: ActorRef = system.actorOf(Props(new ShapesDrawingDriver(drawer)), "drawingService")
    driver ! Start
  }
}


class ShapesDrawingDriver(drawerActor: ActorRef) extends Actor {

  import Message._

  //receive消息，类比与 override def receive: Receive =
  override def receive = {
    case Start =>
      drawerActor ! Circle(Point(), 1.0)
      drawerActor ! Rectangle(Point(), 2, 5)
      drawerActor ! 3.14159
      drawerActor ! Triangle(Point(), Point(2.0), Point(1.0, 2.0))
      drawerActor ! Exit
    case Finished =>
      println(s"ShapesDrawingDriver: cleaning up...")
      context.stop(drawerActor)
    case response: Response =>
      println("ShapesDrawingDriver: Response = " + response)
    case unexpected =>
      println("ShapesDrawingDriver: ERROR: Received an unexpected message = "
        + unexpected)
  }
}
