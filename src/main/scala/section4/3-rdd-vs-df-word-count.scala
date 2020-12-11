package section4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, lower, split}

// warning: BAD example
// we don't have the structure
// RDD would be more straight forward
object WordCount {

  case class Book(value: String)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val path = "dev-data/book.txt"
    val regex = "\\W+"

    // SOLUTION 1:
    val DS = spark.read
      .text(path)
      .as[Book]
    // note: explode + split
    val createRowsForEachWordFunc = explode(split($"value", regex))
    DS
      .select(createRowsForEachWordFunc.as("word"))
      .filter($"word" =!= "")
      .select(lower($"word").as("word"))
      .groupBy("word").count()
      .sort("count")
      .show()

    // SOLUTION 2: (Blending RDD's and Datasets)
    // ToDS:
    val RDD = spark.sparkContext.textFile(path)
    val wordsDS = RDD.flatMap(x => x.split(regex)).toDS()
    wordsDS
      .select(lower($"value").as("word"))
      .groupBy("word").count()
      .sort("count")
      .show()

    spark.stop()
  }
}

