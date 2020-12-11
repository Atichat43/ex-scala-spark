package section4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object TotalSpentByCustomer {

  case class customerOrdersClass(customerID: Int, productID: Int, price: Double)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    val customerOrdersSchema = new StructType()
      .add("customerID", IntegerType, nullable = true)
      .add("productID", IntegerType, nullable = true)
      .add("price", DoubleType, nullable = true)

    import spark.implicits._
    val path = "dev-data/customer-orders.csv"
    val DF = spark.read
      .schema(customerOrdersSchema)
      .csv(path)
      .as[customerOrdersClass]

    DF.printSchema()
    DF.show(2)
    println(DF.count())

    // SOLUTION 1:
    // withColumn
    DF
      .groupBy($"customerID").sum("price")
      .withColumn("total", round($"sum(price)", 2))
      .select("customerID", "total")
      .sort("total")
      .show()

    // SOLUTION 2:
    // agg: AGGREGATION and alias
    DF
      .groupBy($"customerID")
      .agg(round(sum("price"), 2)
        .as("total"))
      .sort("total")
      .show()

    spark.stop()
  }
}

