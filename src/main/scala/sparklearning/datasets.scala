package sparklearning

import scala.util.Random._
import org.apache.spark.sql.SparkSession

object Datasets {
  val spark = SparkSession
    .builder()
    .appName("datasets")
    .master("local[*]")
    .getOrCreate()

  case class Usage(uid: Int, user: String, usage: Int)
  case class UsageCost(uid: Int, user: String, usage: Int, cost: Double)

  def main(args: Array[String]): Unit = {
    val r = new scala.util.Random(42)
    val data = for {
      i <- 0 to 1000
      user = "user-" + r.alphanumeric.take(5).mkString("")
    } yield (Usage(i, user, r.nextInt(1000)))

    import spark.implicits._
    val ds = spark.createDataset(data)

    ds.filter(row => row.usage > 900)

    def filterNineHundred(u: Usage): Boolean = u.usage > 900
    ds.filter(filterNineHundred(_))

    def computeCost(usage: Int): Double = {
      if (usage > 750) usage * 0.15 else usage * 0.50
    }
    ds.map(u => computeCost(u.usage))

    def computeCostColumn(d: Usage): UsageCost = {
      UsageCost(d.uid, d.user, d.usage, computeCost(d.usage))
    }
    ds.map(u => computeCostColumn(u)).show()
  }
}
