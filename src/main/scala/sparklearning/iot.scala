package sparklearning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.types.{StringType, IntegerType, DateType}
import org.apache.spark.sql.{Encoder, Encoders}
import scala.reflect.runtime.universe.TypeTag

object IOT {
  val spark = SparkSession
    .builder()
    .appName("spark_learning")
    .master("local[*]")
    .getOrCreate()

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

  import scala.reflect.runtime.universe._

  implicit val IOTDataTypesTypeTag: TypeTag[IOTDataTypes] =
    typeTag[IOTDataTypes]
  implicit val WarmIOTDataTypesTypeTag: TypeTag[WarmIOTDataTypes] =
    typeTag[WarmIOTDataTypes]

  implicit val IOTDataTypesEncoder: Encoder[IOTDataTypes] =
    Encoders.product[IOTDataTypes]
  implicit val WarmIOTDataTypesEncoder: Encoder[WarmIOTDataTypes] =
    Encoders.product[WarmIOTDataTypes]

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
      .filter(v => { v.temp > 30 && v.out_in == "Out" })

    // Equivalent Dataframe API
    val warm_df = silver_df
      .select("room_id", "out_in", "temp")
      .filter(F.col("temp") > 30 && F.col("out_in") === "Out")

    warm_ds.explain(true)
    warm_df.explain(true)

    spark.sql(s"""
      CREATE TABLE iot_tbl (
        id STRING,
        room_id STRING,
        noted_date STRING, 
        temp INT,
        out_in STRING
      )
      USING csv
      LOCATION '/home/denis/code/spark-learning/data/IOT-temp.csv'
    """)

    spark.catalog.listTables().show()

    spark.sql("SELECT * FROM iot_tbl LIMIT 5").show()

    spark.stop()
  }
}
