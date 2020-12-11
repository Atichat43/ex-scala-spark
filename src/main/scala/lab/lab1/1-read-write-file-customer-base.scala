package lab.lab1

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.DAYS
import java.time.{LocalDate, Month}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random.nextInt

object CustomerBaseMain {
  def randomString(chars: Seq[Char], length: Int, prefix: String = ""): String = {
    val sb = new StringBuilder

    for (_ <- 1 to (length - prefix.length)) {
      val randomNum = nextInt(chars.length)
      sb.append(chars(randomNum))
    }

    prefix + sb.toString()
  }

  def randomMSISDN(): String = {
    randomString(('a' to 'z'), 10, "0")
  }

  def randomCardID(): String = {
    randomString(('A' to 'Z') ++ ('0' to '9'), 15)
  }

  def randomDate(from: LocalDate, to: LocalDate, pattern: String = "dd-MM-yyyy"): String = {
    val diff = DAYS.between(from, to)
    val result = from.plusDays(nextInt(diff.toInt))

    result.format(DateTimeFormatter.ofPattern(pattern))
  }

  def randomBirthday(): String = {
    randomDate(LocalDate.of(1960, Month.JANUARY, 1), LocalDate.of(2010, Month.JANUARY, 1))
  }

  // note: CustomerBase case class
  case class CustomerBase(msisdn: String, card_id: String, province: String, name: String, birth_day: String)

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // toDF()

    println("reading... ./data/provinces-thailand.csv")
    val provincesDF = spark.read
      .option("header", "true")
      .csv("src/main/scala/lab/lab1/data/provinces-thailand.csv")

    println("reading... ./data/names.csv")
    val namesDF = spark.read
      .option("header", "true")
      .csv("src/main/scala/lab/lab1/data/names.csv")

    //    dfProvinces.printSchema()
    //    dfNames.printSchema()

    val provinces = provincesDF.select("Province").collect.map(row => row.getString(0))
    val provinceLen = provinces.length

    val names = namesDF.select("FirstForename").collect.map(row => row.getString(0))
    val namesLen = names.length

    def randomProvince(): String = {
      provinces(nextInt(provinceLen))
    }

    def randomName(): String = {
      names(nextInt(namesLen))
    }

    val numRow = 10 // Math.pow(10, 6).toInt
    val numRowRange = (1 to numRow)
    val customerBases = numRowRange.map(_ => CustomerBase(randomMSISDN(), randomCardID(), randomProvince(), randomName(), randomBirthday()))
    val customerBaseDF = customerBases.toDF()

    println("writing... ./data/customer-base.csv")
    customerBaseDF.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .save("src/main/scala/lab/lab1/data/customer-base.csv")

    spark.stop()
  }
}
