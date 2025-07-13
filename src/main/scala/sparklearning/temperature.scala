package sparklearning
import org.apache.spark.sql.SparkSession

object Temperature {
  def main(Args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("temperature")
      .master("local[*]")
      .getOrCreate()

    val r1 = Array(35, 52, 32, 89, 98)
    val r2 = Array(32, 52,  2,  9, 98, 23)

    import spark.implicits._
    val temp_df = Seq(r1, r2).toDF("celsius")
    temp_df.createOrReplaceTempView("temperature_view")

    spark.sql("""
      SELECT
        celsius,
        transform(celsius, t -> t * 9 / 5 + 32) AS fahrenheit
      FROM temperature_view;
    """).show()

    spark.sql("""
      SELECT
        celsius,
        aggregate(
          celsius,
          0,
          (acc, t) -> acc + t,
          acc -> round(acc / size(celsius) * 9 / 5 + 32, 1)
        ) as fahrenheit_average
      FROM temperature_view;
    """).show()

  }
}
