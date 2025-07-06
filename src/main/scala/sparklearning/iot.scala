package sparklearning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.types.{StringType, IntegerType, DateType}

case class IOTDataTypes(
  id: String,
  room_id: String,
  noted_date: String, 
  temp: Integer,
  out_in: String
)

case class WarmIOTDataTypes(
  room_id: String,
  out_in: String,
  temp: Integer
)

object IOT {
  val spark = SparkSession
    .builder()
    .appName("spark_learning")
    .master("local[*]")
    .getOrCreate()

  val path = "data/IOT-temp.csv"
  val bronze_df = spark.read.option("header", "true").csv(path)
  val silver_df = bronze_df
    .withColumnRenamed("room_id/id", "room_id")
    .withColumnRenamed("out/in", "out_in")
    .withColumn("temp", F.col("temp").cast(IntegerType))
    .withColumn(
      "noted_date",
      F.to_timestamp(F.col("noted_date"), "dd-MM-yyyy HH:mm")
    )

  import spark.implicits._
  val iot_ds = silver_df.as[IOTDataTypes]

  def main(args: Array[String]): Unit = {
    val warm_ds = iot_ds
      .map(v => WarmIOTDataTypes(v.room_id, v.out_in, v.temp))
      .filter(v => {v.temp > 30 && v.out_in == "Out"})
    
    // Equivalent Dataframe API
    val warm_df = silver_df
      .select("room_id", "out_in", "temp")
      .filter(F.col("temp") > 30 && F.col("out_in") === "Out")

    warm_ds.explain(true)
    warm_df.explain(true)
    spark.stop()
  }
}
