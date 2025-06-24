package sparklearning

import org.apache.spark.sql.SparkSession

object IOTData {
  val spark = SparkSession
    .builder()
    .appName("spark_learning")
    .master("local[*]")
    .getOrCreate()

  val path = "data/IOT-temp.csv"
  val raw_df = spark.read.option("header", "true").csv(path)

  raw_df.show()

  // case class IOTDAta(
  //   id: String,
  //   room_id/id: String,
  //   noted_date: Date, 
  //   temp: Float,
  //   out/in: String
  // )

}
