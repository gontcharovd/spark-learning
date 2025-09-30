package sparklearning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{
  StructType,
  StructField,
  StringType,
  IntegerType
}

object Flights {
  val spark = SparkSession
    .builder()
    .appName("flights")
    .master("local[*]")
    .getOrCreate()

  // Must be absolute path to create external table from CSV
  val csvFile = "/home/denis/code/spark-learning/" +
    "data/flights/departuredelays.csv"

  val schema = """`date` STRING, `delay` INT, `distance` INT,
    `origin` STRING, `destination` STRING"""

  val flights_df = spark.read
    .format("csv")
    .option("header", "true")
    .schema(schema)
    .load(csvFile)

  def main(args: Array[String]): Unit = {
    spark.sql("CREATE DATABASE flights_db")
    spark.sql("USE flights_db")

    flights_df.createOrReplaceTempView("flights_view")

    // This will create files in spark-warehouse
    flights_df.write
      .mode("overwrite")
      .saveAsTable("flights_managed_tbl")

    val long_flights_df = spark.sql("""
      SELECT date, distance, origin, destination
      FROM flights_view
      WHERE distance > 1000
      ORDER BY distance DESC
      LIMIT 10
      """)

    // long_flights_df.show()

    // Table from CSV
    spark.sql(s"""
      CREATE TABLE flights_tbl_csv (
        date STRING,
        delay INT,
        distance INT,
        origin STRING,
        destination STRING
      )
      USING csv
      OPTIONS (
        PATH '${csvFile}',
        header 'true'
      )
    """)

    spark.catalog.listTables().show()
    spark.sql("CACHE LAZY TABLE flights_tbl_csv")
    spark.sql("SELECT * FROM flights_tbl_csv LIMIT 10")

    // spark.sql("DROP TABLE flights_managed_tbl")
    // spark.sql("DROP TABLE flights_tbl_csv")
    spark.sql("DROP DATABASE flights_db CASCADE")

    // clean shutdown
    spark.stop()
  }
}
