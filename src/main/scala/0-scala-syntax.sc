// DATA TYPES
/////////////////////////////////////////////////////////////////
val four: Int = 4
val truth: Boolean = "equal" == "equal"
val letterA: Char = 'a'
val hello: String = "hello!"
val pi: Double = 3.14159265
val piSinglePrecision: Float = 3.14159265f
val bigNumber: Long = 123456789
val smallNumber: Byte = 127
println(s"Test: $hello $four")

(1 > 2) & (1<2)
1 == 0 || truth

// FLOW CONTROL
/////////////////////////////////////////////////////////////////
if (false) "if" else "else"

2 match {
  case 1 => println("case: One")
  case 2 => println("case: Two")
  case _ => println("Something else")
}

var x = 0
do { println(x); x+=1 } while (x <= 5)
while (x > 0) { println(x); x -= 1; }
for (x <- 1 to 4) println(x)

// FUNCTIONS
/////////////////////////////////////////////////////////////////
// format def <function name>(parameter name: type...) : return type = { }
// functional programming will return for each expressions
def uppercase(str: String) : String = {str.toUpperCase()}
def transformStr(str: String, func: String => String): String = {func(str)}

transformStr("foo", uppercase)
transformStr("foo", str => str.toUpperCase())

// DATA STRUCTURE
/////////////////////////////////////////////////////////////////
// tuples (immutable lists)
// refer to the individual fields with a ONE-BASED index
val tuples = ("A", 2, true)
println("Tuples ONE-BASED Index: ",tuples._2, tuples)

// lists
// like a tuple, but more functionality with a ZERO-BASED index
// *must be of same type*
val lst = List(1, 2, 3, 4)
val mapLst = lst.map((i: Int) => { i + 10})
val filterLst = lst.filter((x: Int) => x < 3)
val reduceLst = lst.reduce( (x: Int, y: Int) => x + y)

println("List ZERO-BASED Index: ",lst(1), lst.head, lst.tail)
println("reduceLst (sum): ", reduceLst)
for (i <- lst) {println(i)}
for (i <- mapLst) {println(i)}
for (i <- filterLst) {println(i)}

lst ++ List(5,6)
lst.contains(3)
lst.max
lst.sum
lst.reverse
lst.sorted
(lst ++ lst).distinct

// maps (key value) (Dictionary)
val maps = Map("A" -> 1, "B" -> 2)
println(maps("A"))
println(maps.contains("A"))

util.Try(maps("xxx")) getOrElse "Unknown"

// bonus
1.to(10).toList.filter(_ % 2 == 0)