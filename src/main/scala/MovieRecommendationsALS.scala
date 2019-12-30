import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

import scala.io.{Codec, Source}


/** Alternating Least Squares ML approach to the movie recommendation
  * algorithm we wrote from bare bones approach. Essentially train
  * a model given certain parameters. */
object MovieRecommendationsALS {

  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()

     val lines = Source.fromFile("src/main/resources/ml-100k/u.item").getLines()
     for (line <- lines) {
       val fields = line.split('|')
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }

    movieNames
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MovieRecommendationsALS")


    println("Loading movie names...")
    val nameDict = loadMovieNames()

    val data = sc.textFile("src/main/resources/ml-100k/u.data")

    //Rating objects are a part of the MLLib library
    val ratings = data
      .map( x => x.split("\\t") )
       //userid, movieid, rating
      .map( x => Rating(x(0).toInt, x(1).toInt, x(2).toDouble) )
      .cache()

    // Build the recommendation model using Alternating Least Squares
    println("\nTraining recommendation model...")

    val rank = 8
    val numIterations = 20

    val model = ALS.train(ratings, rank, numIterations, 0.01)

    val userID:Int = 196

    println("\nRatings for user ID " + userID + ":")

    val userRatings = ratings
      .filter(_.user == userID)

    val myRatings = userRatings.collect()

    for (rating <- myRatings) {
      println(nameDict(rating.product.toInt) + ": " + rating.rating.toString)
    }

    println("\nTop 10 recommendations:")

    val recommendations = model.recommendProducts(userID, 10)
    for (recommendation <- recommendations) {
      println( nameDict(recommendation.product.toInt) + " score " + recommendation.rating )
    }

    readLine
    sc.stop

  }
}
