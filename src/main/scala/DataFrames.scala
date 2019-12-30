import org.apache.spark.sql._
import org.apache.log4j._
    
object DataFrames {
  
  case class Person(ID:Int,
                    name:String,
                    age:Int,
                    numFriends:Int)
  
  def mapper(line:String): Person = {
    val fields = line.split(',')  
    Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger
      .getLogger("org")
      .setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[8]")
      .getOrCreate

    val lines = spark
      .sparkContext
      .textFile("src/main/resources/RDDFiles/fakefriends.csv")

    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema. (Taking advantage of Spark implicits
    import spark.implicits._
    val people = lines
      .map(mapper)
      .toDS()
      .cache()
    
    // There are lots of other ways to make a DataFrame.
    // For example, spark.read.json("json file path")
    // or sqlContext.table("Hive table name")
    
    println("Here is our inferred schema:")
    people
      .printSchema()
    
    println("Let's select the name column:")
    people
      .select("name")
      .show(5)
    
    println("Filter out anyone over 21:")
    people
      .select("name","numFriends", "age")
      .where("age < 21")
      .show(5)

    //now grab a dataframe
    println("Group by age:")
    val groupedPeople = people
      .groupBy("age")
      .count()

    groupedPeople
      .show(10)

    println("Make everyone 10 years older:")
    people
      .select(people("name"), people("age") +10)
      .show(10)


    readLine
    spark
      .stop
  }
}
