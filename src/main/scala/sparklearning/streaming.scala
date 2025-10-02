package sparklearning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

object Streaming {
  val spark = SparkSession
    .builder
    .appName("streaming")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val lines = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val words = lines
      .select(split(col("value"), "\\s").as("word"))
    
    val counts = words.groupBy("word").count()

    val checkpointDir = "..."
    val streamingQuery = counts.writeStream
      .format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime("1 second"))
      .option("checkpointLocation", checkpointDir)
      .start()

    streamingQuery.awaitTermination()

  }
}
