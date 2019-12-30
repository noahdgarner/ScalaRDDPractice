import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
//okay so the key take away is that DataSets must contain JVM objects
//so we need the class and its mapper object creator below
//And we wnt Datasets/Dataframes because Spark has more API for them

object SparkSQL {

  //what a great reminder of how classes work in scala that is just great, magic constructors (apply method no need for new operator)
  case class Person(ID: Int,
                    name: String,
                    age: Int,
                    numFriends: Int)

  //return a person object to each line of the DataSet (not RDD anymore)
  def mapper(line: String): Person = {
    val fields = line.split(',')
    Person(fields(0).toInt,
      fields(1),
      fields(2).toInt,
      fields(3).toInt)
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
      .master("local[*]")
      // .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate

    //required to convert RDD -> Dataset with .toDS
    import spark.implicits._

    //we use RDD here because Data is unstructured
    val lines: RDD[String] = spark
      .sparkContext
      .textFile("src/main/resources/RDDFiles/fakefriends.csv")

    //create our "structured RDD", and cache because we'll use more than once
    //i.e. we don't want to reprocess the same DAG multiple times
    val peopleDS = lines
      .map(mapper)
      .toDS
      .cache
    //it is now a structured RDD of type Person

    // Infer the schema, and register the DataSet as a table.
    //Must have this line in order to create a DataSet... dumb (included above)
    //import spark.implicits._
    //Why do this to an RDD? Spark has better optimization logic for a
    //Dataset, and more functions, and work with Databases!!
    //old example
    /*val schemaPeople = people
      .toDS*/

    println("Here is our inferred schema (Dataset")
    peopleDS
      .printSchema

    //This will convert our DataSet into a SQL table ie. Dataframe
    //call SQL queries on... WOOOOOWOWWWWWOWWOW with table name people
    //This registers a people table as a view unless it already exists
    peopleDS
      .createOrReplaceTempView("people")


    // SQL can be run over DataFrames that have been registered as a table
    //people table is now a Dataframe, so its the actual table from after the query
    //This is what we call running a SQL query programmatically
    val teenagers = spark
      .sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

    //Since we saved the people table, it is cached until we do above
    val teenagersNot19 = teenagers
      .filter("age != 19")

    teenagersNot19.show


    //now lets convert back to RDD (an RDD of Person objects)
    val peopleRDD = peopleDS.rdd

    //oh we can do awesome filter functions on objects with filter, totally forgot
    val mappedPeople = peopleRDD
      .filter(x => x.age > 60)
      .map(x => (x, 1))


    mappedPeople
      .collect
      .foreach(println)

    //so we can see web UI
    readLine
    //exactly the same as closing a database connection on other languages
    spark
      .stop
  }
}
