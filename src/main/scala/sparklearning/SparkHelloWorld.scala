package sparklearning

import org.apache.spark.sql.SparkSession

object SparkHelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Hello_World")
      .master("local[*]")
      .getOrCreate()

    val path = "src/main/resources/data/country_full.csv"
    val df = spark.read.option("header", "true").csv(path)

    df.show()
  }
}
