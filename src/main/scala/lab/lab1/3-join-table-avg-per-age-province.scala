package lab.lab1

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.DAYS
import java.time.{LocalDate, Month}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object AvgPerAgeGroup {
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

    println("reading... ./data/purchase-transaction.csv")
    val purchaseTransactionDF = spark.read
      .option("header", "true")
      .csv("src/main/scala/lab/lab1/data/purchase-transaction.csv")

    // note: udf
    val formatDateToBDYearUDF = udf {
      date: String =>
        (
          LocalDate.parse(date, DateTimeFormatter.ofPattern("dd-MM-yyyy")).getYear + 543
          ).toInt
    }

    val formatMSISDNTo66 = udf {
      id: String => id.replaceFirst("0", "66")
    }

    val getAgeFromBDYearUDF = udf {
      year: Int => LocalDate.now.getYear + 543 - year
    }

    val getAgeGroupUDF = udf {
      age: Int => if (age == 60) 5 else ((age - 10 + 1) / 10.0).ceil.toInt
    }

    val formattedCustomerBase = customerBaseDF
      .withColumn("msisdn", formatMSISDNTo66(col("msisdn")))
      .withColumn("birth_day", formatDateToBDYearUDF(col("birth_day")))
      .withColumn("age", getAgeFromBDYearUDF(col("birth_day")))
      .withColumnRenamed("birth_day", "bd_year")

    val minMax = formattedCustomerBase
      .agg(min(col("age")), max(col("age")))
      .head()

    val minAge = minMax.getInt(0)
    val maxAge = minMax.getInt(1)
    val numGroup = 5
    val divider = ((maxAge - minAge) / numGroup).ceil.toInt

    val getAgeGroupLabelUDF = udf { ageGroup: Int =>
      (ageGroup * 10).toString + " - " + (if (ageGroup == 5) 60 else (ageGroup + 1) * 10 - 1).toString
    }

    // note: final customer base df
    val finalCustomerBaseDF = formattedCustomerBase
      .withColumn("age_group", getAgeGroupUDF($"age"))

    // note: validate
    println("validation: no customerBase age_group < 1 || age_group > 5")
    finalCustomerBaseDF.filter($"age_group" < 1 || $"age_group" > numGroup).show()

    val finalPurchaseTransactionDF = purchaseTransactionDF
      .groupBy("card_id")
      .agg(sum("amount").as("total"))

    val customerBaseJoinPurchaseTransactionDF = finalCustomerBaseDF.join(
      finalPurchaseTransactionDF,
      finalCustomerBaseDF("card_id") === finalPurchaseTransactionDF("card_id"),
      "inner"
    )

    val avgAmountPerAgeGroup = customerBaseJoinPurchaseTransactionDF
      .groupBy("age_group")
      .agg(avg("total").as("average_total"))
      .select("age_group", "average_total").sort("age_group")

    val resultAvgPerAgeGroup = avgAmountPerAgeGroup
      .withColumn("age_group_label", getAgeGroupLabelUDF(col("age_group")))
      .select("age_group", "age_group_label", "average_total")

    val resultAvgPerProvince = customerBaseJoinPurchaseTransactionDF
      .groupBy("province")
      .agg(avg("total").as("average_total"))
      .select("province", "average_total")

    println("writing... avg-per-age-group.csv")
    resultAvgPerAgeGroup.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .save("src/main/scala/lab/lab1/data/avg-per-age-group.csv")

    println("writing... avg-per-province.csv")
    resultAvgPerProvince.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .save("src/main/scala/lab/lab1/data/avg-per-province.csv")

    spark.stop()
  }
}