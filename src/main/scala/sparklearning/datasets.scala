package sparklearning

import scala.util.Random._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Encoder, Encoders}
import scala.reflect.runtime.universe.TypeTag

object Datasets {
  val spark = SparkSession
    .builder()
    .appName("datasets")
    .master("local[*]")
    .getOrCreate()

  case class Usage(uid: Int, user: String, usage: Int)
  case class UsageCost(uid: Int, user: String, usage: Int, cost: Double)

  // Correct way to create TypeTags in Scala 3
  import scala.reflect.runtime.universe._
  
  implicit val usageTypeTag: TypeTag[Usage] = typeTag[Usage]
  implicit val usageCostTypeTag: TypeTag[UsageCost] = typeTag[UsageCost]

  // Create explicit encoders (these should work now with TypeTags)
  implicit val usageEncoder: Encoder[Usage] = Encoders.product[Usage]
  implicit val usageCostEncoder: Encoder[UsageCost] = Encoders.product[UsageCost]

  def main(args: Array[String]): Unit = { 
    val r = new scala.util.Random(42)
    val data = for {
      i <- 0 to 1000
      user = "user-" + r.alphanumeric.take(5).mkString("")
    } yield Usage(i, user, r.nextInt(1000))  // Fixed: removed extra parentheses
    
    import spark.implicits._
    
    val ds = spark.createDataset(data)
    
    // Show some basic operations
    println("Original dataset:")
    ds.show(5)
    
    // Filter operations
    println("Usage > 900:")
    ds.filter(row => row.usage > 900).show(5)
    
    def filterNineHundred(u: Usage): Boolean = u.usage > 900
    println("Using filter function:")
    ds.filter(filterNineHundred(_)).show(5)
    
    def computeCost(usage: Int): Double = {
      if (usage > 750) usage * 0.15 else usage * 0.50
    }
    
    // Map to costs only
    println("Just the costs:")
    ds.map(u => computeCost(u.usage)).show(5)
    
    def computeCostColumn(d: Usage): UsageCost = {
      UsageCost(d.uid, d.user, d.usage, computeCost(d.usage))
    }
    
    // Map to UsageCost objects
    println("Usage with costs:")
    ds.map(u => computeCostColumn(u)).show(10)
    
    spark.stop()
  }
}
