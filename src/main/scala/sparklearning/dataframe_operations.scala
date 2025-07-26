package sparklearning
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}
import Flights.flights_df

object Airports {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("flights")
      .master("local[*]")
      .getOrCreate()

    val airports_path = "data/flights/" +
      "airport-codes-na.txt"
    val airports_df = spark
      .read
      .option("lineSep", "\r")
      .option("sep", "\t")
      .option("header", "true")
      .csv(airports_path)
    airports_df.createOrReplaceTempView("airports_na")
   
    val delays = flights_df
    delays.createOrReplaceTempView("departureDelays")

    val foo_df = delays
      .filter(
        F.col("origin") === "SEA" &&
        F.col("destination") === "SFO" &&
        F.col("date").like("01010%") &&
        F.col("delay") > 0
      )
      foo_df.createOrReplaceTempView("foo")

    // For each origin airport, find the three
    // destinations with the most delays
    spark.sql("""
      WITH departureDelaysTotals AS (
        SELECT origin, destination, SUM(delay) AS delayTotal
        FROM departureDelays
        WHERE
          origin IN ('SEA', 'SFO', 'JFK') AND 
          destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX','ATL')
        GROUP BY origin, destination
      )
      SELECT origin, destination, delayTotal, rank
      FROM 
        (
          SELECT
            origin,
            destination,
            delayTotal,
            dense_rank() OVER (
              PARTITION BY origin
              ORDER BY delayTotal DESC
            ) as rank
          FROM departureDelaysTotals
        )
      WHERE rank <= 3
    """).show()
  }
}
