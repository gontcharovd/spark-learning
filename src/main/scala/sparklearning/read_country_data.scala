package sparklearning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

object CountryData {
    val spark = SparkSession
      .builder()
      .appName("spark_learning")
      .master("local[*]")
      .getOrCreate()

    val path = "src/main/resources/data/country_full.csv"
    val df = spark.read.option("header", "true").csv(path)
    val count_df = df
      .where(functions.col("region").isNotNull)
      .groupBy("region")
      .agg(functions.count("region"))
}
