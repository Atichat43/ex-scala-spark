import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object TemplateSpark {

  case class TempCaseClass(id: Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val path = "dev-data/temp.csv"
    val DF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .as[TempCaseClass]

    DF.printSchema()
    DF.show(2)
    println(DF.count())

    spark.stop()
  }
}
