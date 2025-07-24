package sparklearning

object PatternMatching {
  def main(args: Array[String]): Unit = {
    def numbers(x: Int): String = x match {
      case 1 => "one"
      case 2 => "two"
      case _ => "other"
    }

    val two = numbers(2)
    println(two)
  }
}
