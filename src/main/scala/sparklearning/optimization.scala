package sparklearning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}

object Optimization {
  val spark = SparkSession
    .builder()
    .appName("optimization")
    .config("spark.sql.shuffle.partitions", "300")
    .config("spark.shuffle.file.buffer", "1MB")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    spark
      .sql("SET -v")
      .filter(F.col("key").contains("spark.sql.shuffle.partitions"))
      .select("key", "value")
      .show(5, false)
  }

}
