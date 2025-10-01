package sparklearning

import org.apache.spark.sql.SparkSession
import scala.collection.immutable.Map

object JoinsExercise {
  val spark = SparkSession.builder
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .master("local[*]")
    .appName("JoinsExercise")
    .getOrCreate()

  def printConfig(session: SparkSession, key: String) = {
    val mconf = session.conf.getAll
    println(s"${key}: ${mconf(key)}")
  }

  def main(args: Array[String]): Unit = {
    printConfig(spark, "spark.sql.autoBroadcastJoinThreshold")

    val range = 100000
    val rnd = new scala.util.Random(123)
    val countries = Map(
      0 -> "Belgium",
      1 -> "Germany",
      2 -> "Canada",
      3 -> "Denmark",
      4 -> "Estonia"
    )
    val items = Map(
      0 -> "item_A",
      1 -> "item_B",
      2 -> "item_C",
      3 -> "item_D",
      4 -> "item_E"
    )

    import spark.implicits._

    val usersDF = (0 to range)
      .map(x =>
        (
          x,
          s"user_${x}",
          countries(rnd.nextInt(5))
        )
      )
      .toDF("id", "name", "country")

    val ordersDF = (0 to range)
      .map(x =>
        (
          s"order_${x}",
          rnd.nextInt(range),
          items(rnd.nextInt(5))
        )
      )
      .toDF("order_id", "user_id", "item_name")

    // Shuffle sort merge join without bucketing
    val userOrdersDF = usersDF.join(ordersDF, $"id" === $"user_id")
    userOrdersDF.show()

    // Shuffle sort merge join with bucketting
    val numBuckets = 16
    usersDF
      .orderBy("id")
      .write
      .mode("overwrite")
      .bucketBy(numBuckets, "id")
      .saveAsTable("usersTable")

    ordersDF
      .orderBy("user_id")
      .write
      .mode("overwrite")
      .bucketBy(numBuckets, "user_id")
      .saveAsTable("ordersTable")

    val usersBucketDF = spark.read.table("usersTable")
    val ordersBucketDF = spark.read.table("ordersTable")

    val userOrdersBucketDF =
      usersBucketDF.join(ordersBucketDF, $"id" === $"user_id")
    userOrdersBucketDF.show()

    spark.sql("DROP TABLE usersTable")
    spark.sql("DROP TABLE ordersTable")
    spark.stop()
  }
}
