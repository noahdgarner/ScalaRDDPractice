import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

import scala.io.{Codec, Source}

/** Find the movies with the most ratings. */
object PopularMoviesDataSets {

  /** Load up a Map of movie IDs to movie names. The less inputs the better */
  def loadMovieNames(): Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames: Map[Int, String] = Map()

    val lines = Source.fromFile("src/main/resources/ml-100k/u.item").getLines()
    for (line <- lines) {
      val fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    movieNames
  }

  // Case class so we can get a column name for our movie ID
  final case class Movie(movieID: Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger
      .getLogger("org")
      .setLevel(Level.ERROR)

    val textFile:String = "u.data"

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("PopularMovies")
      .master("local[*]")
      .getOrCreate()

    val namesDict = spark
      .sparkContext
      .broadcast(loadMovieNames)

    // Read in each rating line and extract the movie ID; construct an RDD of Movie objects.
    val lines = spark
      .sparkContext.textFile(s"src/main/resources/ml-100k/$textFile")
      .map(x => Movie(x.split("\t")(1).toInt))

    // Convert to a DataSet
    import spark.implicits._
    val moviesDS = lines
      .toDS()

    // Some SQL-style magic to sort all movies by popularity in one line!
    val topMovieIDs = moviesDS
      .groupBy("movieID")
      .count()
      .orderBy(desc("count"))
      .cache()

    println("MOVIES VIEW with SQL")
    val topMoviesView = topMovieIDs
      .createOrReplaceTempView("MovieIDs")


    val queriedMovies = spark
      .sql("select * from MovieIDs where count < 400")

    queriedMovies
      .take(10)
      .foreach(println)

    // Show the results at this point:
    /*
    |movieID|count|
    +-------+-----+
    |     50|  584|
    |    258|  509|
    |    100|  508|
    */


    println("Showing The rest of the data as per ")
    topMovieIDs
      .show()

    topMovieIDs
      .printSchema()

    // Grab the top 10
    val top10 = topMovieIDs
      .take(10)

    println("Printing to see what rows look like")
    top10
      .foreach(println)

    // Print the results
    println
    for (result <- top10) {
      // result is just a Row at this point; we need to cast it back.
      //(Only because we are quering a map key to get a value, JAva is the same)
      // Each row has movieID, count as above.
      println(namesDict.value(result(0).asInstanceOf[Int])+": "+result(1).asInstanceOf[Long])
    }

    // Stop the session
    readLine
    spark
      .stop
  }
}
