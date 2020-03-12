package com.research.patternmatch

/**
  * @fileName: CaseApp.java
  * @description: case class匹配
  * @author: by echo huang
  * @date: 2020-03-12 21:56
  */
object CaseApp extends App {

  def caseClassMatch(person: Person1): Unit = {
    person match {
      case cto: CTO => println(cto)
      case employee: Employee => println(employee)
      case other: Other => println(other)
    }
  }


  caseClassMatch(CTO("hsm", "good"))

  caseClassMatch(Employee("longfuyou", "o"))

  caseClassMatch(Other("tom"))

}


class Person1

case class CTO(name: String, floor: String) extends Person1 {
  override def toString: String = name + floor
}

case class Employee(name: String, floor: String) extends Person1 {
  override def toString: String = name + floor
}

case class Other(name: String) extends Person1 {
  override def toString: String = name
}
