package section4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkSQLMain {

  case class Person(id: Int, name: String, age: Int, friends: Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // toDF()
    val friendsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("dev-data/fakefriends.csv")
      .as[Person]

    friendsDF.printSchema()
    friendsDF.show(2)

    val viewName = "friendsView"
    friendsDF.createOrReplaceTempView(viewName)

    val teenagerDF = spark.sql(s"SELECT * FROM $viewName WHERE age >= 13 AND age <= 19")
    teenagerDF.show()

    spark.stop()
  }
}
