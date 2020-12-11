package section4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object MinTemperatures {

  case class Temperature(stationID: String, date: Int, measure_type: String, temperature: Float)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // note: define structure type
    // in case no header in file
    val temperatureSchema = new StructType()
      .add("stationID", StringType, nullable = true)
      .add("date", IntegerType, nullable = true)
      .add("measure_type", StringType, nullable = true)
      .add("temperature", FloatType, nullable = true)

    import spark.implicits._
    val path = "dev-data/temperatures.csv"
    val DF = spark.read
      .schema(temperatureSchema)
      .option("inferSchema", "true")
      .csv(path)
      .as[Temperature]

    DF.printSchema()
    DF.show(2)
    println(DF.count())


    def convertToFahrenheit(col: Column): Column = {
      col * 0.1f * (9.0f / 5.0f) + 32.0f
    }

    // SOLUTION 1:
    // withColumn: return new Dataset by adding/replace column
    DF
      .groupBy($"stationID").min("temperature")
      .withColumn("minT", round(convertToFahrenheit($"min(temperature)"), 2))
      .select("stationID", "minT")
      .show()

    // SOLUTION 2:
    // select: cols: Column*
    DF
      .groupBy($"stationID").min("temperature")
      .select(
        $"stationID",
        round(convertToFahrenheit($"min(temperature)"), 2).as("minT"))
      .sort("minT")
      .show()

    spark.stop()
  }
}
