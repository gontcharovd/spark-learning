package sparklearning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

object LearningSpark {
  def main(args: Array[String]): Unit = {

    val count_df = CountryData.count_df
    count_df.show()

    val row = RowHelper.row
    println(row)
    
    val element = row.getInt(0)
    println(element)

    val iot_df = IOTData.raw_df
    iot_df.show()

  }
}
