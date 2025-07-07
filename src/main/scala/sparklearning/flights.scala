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
      StructField("date", StringType),
      StructField("delay", IntegerType),
      StructField("distance", IntegerType),
      StructField("origin", StringType),
      StructField("destination", StringType)
    )
  )

  val schema_str = """`date` STRING, `delay` INT, `distance` INT,
    `origin` STRING, `destination` STRING"""

  val flights_df = spark.read
    .option("header", "true")
    .schema(schema_str)
    .csv(csvFile)

  flights_df.createOrReplaceTempView("flights_tbl")

  def main(args: Array[String]): Unit = {
    val head_df = spark.sql("SELECT * FROM flights_tbl LIMIT 5")

    val long_flights_df = spark.sql("""
      SELECT date, distance, origin, destination
      FROM flights_tbl
      WHERE distance > 1000
      ORDER BY distance DESC
      LIMIT 10
      """)

    // long_flights_df.show()
    
    spark.sql("""
      CREATE TABLE us_delay_flights_tbl(
        date STRING,
        delay INT,
        distance INT,
        origin STRING,
        destination STRING
      )
      USING csv
      OPTIONS (PATH 'data/flights/departuredelays.csv')
    """)

    val us_delay_flights_tbl = spark.sql("SELECT * FROM us_delay_flights_tbl LIMIT 5")

    us_delay_flights_tbl.show()

    // clean shutdown
    spark.stop()
  }
}


