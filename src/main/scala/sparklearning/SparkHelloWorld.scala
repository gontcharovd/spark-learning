package sparklearning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg

object SparkHelloWorld {
  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark = SparkSession.builder()
      .appName("Spark Hello World")
      .master("local[*]") // Use all available cores locally
      .getOrCreate()

    // Import implicits for DataFrames
    import spark.implicits._

    println("Hello, Spark World!")
    println("=" * 50)

    // Create a simple DataFrame
    val data = Seq(
      ("Alice", 25, "Engineer"),
      ("Bob", 30, "Data Scientist"),
      ("Charlie", 35, "Manager"),
      ("Diana", 28, "Analyst"),
      ("Eve", 32, "Developer")
    )

    val df = data.toDF("name", "age", "job")

    println("Sample DataFrame:")
    df.show()

    // Perform some basic transformations
    println("People aged 30 and above:")
    val adults = df.filter($"age" >= 30)
    adults.show()

    // Group by job and count
    println("Job distribution:")
    df.groupBy("job").count().show()

    // Calculate average age
    val avgAge = df.agg(avg($"age")).collect()(0)(0)
    println(s"Average age: $avgAge")

    // Create an RDD example
    val numbersRDD = spark.sparkContext.parallelize(1 to 100)
    val sum = numbersRDD.reduce(_ + _)
    println(s"Sum of numbers 1-100: $sum")

    // Show Spark configuration
    println("\nSpark Configuration:")
    println(s"Application Name: ${spark.conf.get("spark.app.name")}")
    println(s"Master: ${spark.conf.get("spark.master")}")
    println(s"Spark Version: ${spark.version}")

    // Stop the Spark session
    spark.stop()

    println("\nSpark session stopped. Hello World completed successfully!")
  }
}
