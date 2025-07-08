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

  val schema= """`date` STRING, `delay` INT, `distance` INT,
    `origin` STRING, `destination` STRING"""

  val flights_df = spark.read
    .option("header", "true")
    .schema(schema)
    .csv(csvFile)

  def main(args: Array[String]): Unit = {
    spark.sql("CREATE DATABASE flights_db")
    spark.sql("USE flights_db")

    flights_df.createOrReplaceTempView("flights_view")

    val long_flights_df = spark.sql("""
      SELECT date, distance, origin, destination
      FROM flights_view
      WHERE distance > 1000
      ORDER BY distance DESC
      LIMIT 10
      """)

    // long_flights_df.show()

    // Table from CSV
    // This doesn't work
    spark.sql(s"""
      CREATE TABLE flights_tbl_csv (
        date STRING,
        delay INT,
        distance INT,
        origin STRING,
        destination STRING
      )
      USING com.databricks.spark.csv
      OPTIONS (
        PATH '${csvFile}',
        header 'true'
      )
    """)

    spark.catalog.listTables().show()

    val df = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(csvFile)

    df.show()

    // clean shutdown
    spark.stop()
  }
}
