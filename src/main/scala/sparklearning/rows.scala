package sparklearning

import org.apache.spark.sql.Row

object RowHelper {
  val row = Row(350, true, "Learning Spark 2E", null)

  def main(args: Array[String]): Unit = {
    println(row)
    val element = row.getInt(0)
    println(element)
  }
}
