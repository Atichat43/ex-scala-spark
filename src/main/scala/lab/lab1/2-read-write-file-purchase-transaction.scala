package lab.lab1

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.DAYS
import java.time.{LocalDate, Month}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random.nextInt
import scala.util.Random.nextFloat

object PurchaseTransactionMain {
  def randomDate(from: LocalDate, to: LocalDate, pattern: String = "dd-MM-yyyy"): String = {
    val diff = DAYS.between(from, to)
    val result = from.plusDays(nextInt(diff.toInt))

    result.format(DateTimeFormatter.ofPattern(pattern))
  }

  def randomTransactionDate(): String = {
    randomDate(LocalDate.of(2020, Month.JUNE, 1), LocalDate.of(2020, Month.JUNE, 5), "dd/MM/yyyy")
  }

  def randomAmount(): Float = {
    "%.01f".format(nextFloat() * (1000.0 - 100.0) + 100.0).toFloat
  }

  // note: PurchaseTransaction case class
  case class PurchaseTransaction(card_id: String, product_id: Int, date: String, amount: Float)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // toDF()

    println("reading... ./data/customer-base.csv")
    val customerBaseDF = spark.read
      .option("header", "true")
      .csv("src/main/scala/lab/lab1/data/customer-base.csv")


    val cardIDs = customerBaseDF.select("card_id").collect.map(row => row.getString(0))
    val cardIDsLength = cardIDs.length

    def randomCardID(): String = {
      cardIDs(nextInt(cardIDsLength))
    }

    def randomProductID(): Int = {
      nextInt(999)
    }

    val numRow = 10 // 5 * Math.pow(10, 6).toInt
    val numRowRange = (1 to numRow)

    val purchaseTransactions = numRowRange.map(_ => PurchaseTransaction(randomCardID(), randomProductID(), randomTransactionDate(), randomAmount()))
    val purchaseTransactionDF = purchaseTransactions.toDF()

    println("writing... ./data/purchase-transaction.csv")
    purchaseTransactionDF.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .save("src/main/scala/lab/lab1/data/purchase-transaction.csv")

    spark.stop()
  }
}
