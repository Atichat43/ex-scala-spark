import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, udf}

(1 to Math.pow(10, 6).toInt)

//val lst = List(1, 2, 3, 4)
//lst: _*

def echo(args: String*) =
  for (arg <- args) println(arg)

val arr = Array("What's", "up", "doc?")
echo(arr: _*)

val testUDF = udf {
  total: Int => total + 1000
}

val lst = Array(("total", 2), ("total2", 1))
lst(0)
lst(1)

def something(args: Array[(String, Int)]): Unit = {
  for (arg <- args) {
    println(arg)
    println("--------")
  }
}

something(lst)