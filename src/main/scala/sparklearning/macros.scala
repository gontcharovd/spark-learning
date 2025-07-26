package sparklearning

import scala.quoted.*

object Macros {
  // pattern matching
  def numbers(x: Int): String = x match {
    case 1 => "one"
    case 2 => "two"
    case _ => "other"
  }

  // Macro definition (inline function)
  inline def createGreeting(): String = ${ createGreetingImpl() }
  
  // Macro implementation
  def createGreetingImpl()(using Quotes): Expr[String] = {
    val greeting = Expr("Hello")
    val target = Expr("World")
    val combined = '{ $greeting + " " + $target }
    combined
  }

  def main(args: Array[String]): Unit = {
    val two = numbers(2)
    println(two)
  }
}
