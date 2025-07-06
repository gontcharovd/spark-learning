package sparklearning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

object Flights {
  val spark = SparkSession
    .builder()
    .appName("flights")
    .master("local[*]")
    .getOrCreate()

  val csvFile = "data/flights/departuredelays.csv"

  val schema = StructType(
    Array(
      StructField("date", IntegerType),
      StructField("delay", IntegerType),
      StructField("distance", IntegerType),
      StructField("origin", StringType),
      StructField("destination", StringType)
    )
  )

  val schema_str = """`date` INT, `delay` INT, `distance` INT,
    `origin` STRING, `destination` STRING"""

  val flights_df = spark.read
    .option("header", "true")
    .schema(schema_str)
    .csv(csvFile)

  flights_df.createOrReplaceTempView("flights_tbl")

  def main(args: Array[String]): Unit = {
    val head_df = spark.sql("SELECT * FROM flights_tbl LIMIT 5")
    head_df.show()

    // clean shutdown
    spark.stop()
  }
}


