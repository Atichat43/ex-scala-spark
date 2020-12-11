package section4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FriendsByAge {

  case class FakeFriends(id: Int, name: String, age: Int, friends: Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val path = "dev-data/fakefriends.csv"
    val DF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .as[FakeFriends] // Datasets
      .select("age", "friends") // DataFrame

    DF.printSchema()
    DF.show(2)
    println(DF.count())

    val columnA = col("A").alias("A2")
    val columnB = col("B").as("B2")
    val columnC = $"C"
    val columnCondition = col("age") < 21 // column
    val columnCondition2 = $"D" < 21

    println("columnA: ", columnA)
    println("columnB: ", columnB)
    println("columnC: ", columnC)

    DF.filter("age < 21")
    DF.select("age").filter(columnCondition)
    DF.select("age").filter(columnCondition2)
    DF.select(DF("age"), DF("friends") + 1000).show() //cols: Column*

    // agg: Compute aggregates and returns the result as a DataFrame.
    // built-in: avg, max, min, sum, count
    DF.groupBy("age").count().sort().show(3)

    DF.groupBy($"age" < 50).count().sort().show(3)
    DF.groupBy(col("age") < 50).count().sort().show(3)

    DF.groupBy("age").avg("friends").sort("age").show(3)
    DF.groupBy("age").agg(round(avg("friends"), 2).as("friends_avg")).show(3)

    val results = DF.collect() // array
    println(results.length)

    spark.stop()
  }
}
