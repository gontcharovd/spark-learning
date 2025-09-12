package sparklearning

import scala.util.Random
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

object Joins {
  val spark = SparkSession
    .builder()
    .appName("spark_learning")
    .master("local[*]")
    .getOrCreate()

    // disable broadcast hash join
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold","-1")

  def main(args: Array[String]): Unit = {
    // Generate some sample data for two data sets
    var states = scala.collection.mutable.Map[Int, String]()
    var items = scala.collection.mutable.Map[Int, String]()
    val rnd = new scala.util.Random(42)
    
    // Initialize states and items purchased
    states += (0 -> "AZ", 1 -> "CO", 2-> "CA", 3-> "TX", 4-> "NY", 5-> "MI")

    items += (0 -> "SKU-0", 1 -> "SKU-1", 2-> "SKU-2", 3->"SKU-3", 4 -> "SKU-4",5-> "SKU-5")

    // Import implicits to enable toDF method
    import spark.implicits._

    // Create DataFrames
    val usersDF = (0 to 1000000)
      .map(id => (
        id, 
        s"user_${id}", 
        s"user_${id}@databricks.com", 
        states(rnd.nextInt(5))
      ))
      .toDF("uid", "login", "email", "user_state")

    val ordersDF = (0 to 1000000)
      .map(r => (
        r, 
        rnd.nextInt(100), 
        rnd.nextInt(10000), 
        10 * r * 0.2d, 
        states(rnd.nextInt(5)), 
        items(rnd.nextInt(5))
      ))
      .toDF("transaction_id", "quantity", "users_id", "amount", "state", "items")
    
    // Do the join
    val usersOrdersDF = ordersDF.join(usersDF, $"users_id"=== $"uid")

    // Show the joined results
    usersOrdersDF.show(false)

    spark.stop()
  }

}
