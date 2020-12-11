package lab.lab2

import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.DAYS
import java.time.{LocalDate, Month}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object CRDLocation {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // toDF()

    val ageTarget = 50
    val cdr_tbl = Array(
      "temp",
      "temp2",
    )

    // condition to put inside the get_df function
    val cdr_cond = col("age") < ageTarget
    val cdr_col = List(col("name"), col("age"))
    val cdr_col_2 = List(col("total"))
    val cdr_total = cdr_col ++ cdr_col_2

    val testUDF = udf {
      total: Int => total + 1000
    }

    val testNew = udf {
      age: Int => age + 200
    }

    val testTT = List(
      ("total", testUDF(col("total"))),
      ("new", testNew(col("age")))
    )

    def getDF(tbl: Array[String], cond: Column, col: List[Column], add_col: List[(String, Column)]): DataFrame = {
      val dfs = tbl.map((t: String) => {
        var df = spark.read
          .option("header", "true")
          .csv(s"src/main/scala/lab/lab2/data/$t.csv")
          .where(cond)
          .select(col: _*)

        for (c <- add_col) {
          df = df.withColumn(c._1, c._2)
        }

        df
      })

      dfs.reduce((acc, cur) => cur.union(acc))
    }

    val df = getDF(cdr_tbl, cdr_cond, cdr_total, testTT)
    print(df.count())
    df.show()

    //    println("reading... ./data/temp.csv")
    //    var dataDF = spark.read
    //      .option("header", "true")
    //      .csv("src/main/scala/lab/lab2/data/temp.csv")
    //      .where(cdr_cond)
    //      .select(cdr_total: _*)
    //    //      .select("new", testNew(col("age")))
    //    //      .withColumn(testTT._1, testTT._2)
    //
    //    for (temp <- testTT) {
    //      dataDF = dataDF.withColumn(temp._1, temp._2)
    //    }
    //
    //    dataDF.show()


    //    val cdr_add_col = {
    //      'start_date_time': date_trunc('hour', col('start_datetime')).alias('start_date_time'),
    //      'day_of_week': (dayofweek(col('start_datetime'))-1).alias('day_of_week'),
    //      'hour': hour(col('start_datetime')).alias('hour'),
    //      'par_month': substring(col('par_day'), 1, 6).alias('par_month'),
    //      'par_hour': lpad(hour(col('start_datetime')), 2, '0').alias('par_hour')
    //    }


    spark.stop()
  }
}
