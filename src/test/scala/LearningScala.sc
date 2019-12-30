//Note all primitives are capitalized
//every keyword seems to be capitalized
val hello = "Hello World"

//notice vars can change, mutable
var helloThere = hello
helloThere += "Dicks"


//for loops
for (i <- 0 to helloThere.length) {
  println(i);
}

//match statements
val num = 3;
num match {
  case 1 => println(1)
  case 2 => println(2)
  case 3 => println(3)
}

//while loops
var x = 10
while (x > 0) {
  x -= 1
}

//expressions, returns final value
{val x = 10; x+20}
//prints 30
println({val x = 10; x+20})

//functions, implicit returns
def squareIt(x: Int)  = {
  //note implicit returns
  x*x
}

def main(x: Int): Unit = {
  //note a void function is written 'unit'
}

//anonymous functions, note that its same
//as JavaScript, just more verbose
def transformInt(x: Int, f: Int => Int): Int ={
  f(x)
}
val squared = transformInt(5, squareIt)
println(squared)

//more practice with scala functions
//very interesting language
def transform(x: Int, f: Int => Int): Int = {
  var y = f(x)
  y+=2
  y
}
//note we can pass in unamed functions too
transform(3, x => x*x*x)

//Tuples DS (hold item of any type)
val captains = ("Noah", "Garner", "John")
//ONE based index. Fuck. Note extraction
println(captains._1)
//creating Key/Value pairs
val NoahStuff = "Noah" -> "Katie"
//prints the key
println(NoahStuff._1)
//prints the value
println(NoahStuff._2)
//above is just a 2 element Tuple, but it
//is how Key/Val mappings are made in
//Scala
//This is important because we can map
//any types to each other as K/V pairs
val NoahBDate = "Noah" -> 1994
val NoahBDateList = ("Noah", 1994)

//Lists, like Tuple, but an actual collection
//of objects, more functionality
//singly linked list under hood
//cannot have different types
val myList = List("Carrot", "apple", "cake")
//shit, Lists are 0 index based
println(myList(0))
//head & tail ive 1st, and remainder ele
println(myList.head)
println(myList.tail)
//iterating through lists
for (food <- myList){
  println(food)
}

//functions on lists
//apply function literal to list
//map() used to apply a func to every list item
val backwards = myList
  .map((food: String) => {food.reverse})
//reduce method
val numList = List(1,2,3,4)
val sumList = numList
  .reduce((x,y) => x+y)
//filter method to get a sublist,
//notice similarity to JavaScript
val filteredNums = numList
  //return even numbers
  .filter(x => x%2==0)
//plus plus operator for combining lists
val moreNumbers = List(3,4,5,6)
val allNumbers = numList ++ moreNumbers
//the dot reverse for reversing list
val reversed = allNumbers.reverse
//print only distinct values of list
val distinctNumbers = allNumbers.distinct
//find max
val maxInList = distinctNumbers.max
//sum the list
val summedList = distinctNumbers.sum
//return true or false if contains
val contains3 = allNumbers.contains(3)

//creating a Map DS
val myMap =
  Map("Noah" -> 1994,
      "Doug" -> 1967,
      "Brady" -> 1996)
