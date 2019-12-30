import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.{Row, SparkSession}


//finding it is much easier to strip data out of a json with select, then
//convert with a mapper function from row to RDD types, since this way
//we know the tuple elements without having to search for them. People are
//genius

//This is what I wanted to do. This is a perfect example of where I can
//convert data to RDD, and reduce low level to get averages, then bring it
//back to a dataframe so that I can do some sql queries with it

//IT WORRKEEDDD!!!!!!!!!!!!!! Successfully worked with Datasets, dataframes
//and map reduce tech to map two files between each other
object AverageRestaurantRatings {

  //create a tuple, RDD element contains (userID, (businessID, rating))
  def reviewMapper(row: (String, Double)): (String, Double) = {
    (row._1, row._2)
  }

  def businessMapper(row: Row): (String, String) = {
    (row(0).asInstanceOf[String], row(1).asInstanceOf[String])
  }


  def main(args: Array[String]): Unit = {

    Logger
      .getLogger("org")
      .setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("AverageRestaurantRartings")
      .master("local[8]")
      .getOrCreate

    import spark.implicits._

    val reviewData = spark
      .read
      .json("src/main/resources/RDDFiles/review.json")
      .cache

    val businessData = spark
      .read
      .json("src/main/resources/RDDFiles/business.json")
      .cache

    //pick our columns from json, then name our structured data
    val businessDF = businessData
      .select("business_id", "name")
      //if we don't do this the column names are v._1, v._2..
      .toDF("business_id", "name")


    val idStarsRDD = reviewData
      .select("business_id", "stars")
      .as[(String, Double)]
      .rdd
      .map(reviewMapper)
    //RDD: (business_id, stars) : (String, Double)

    //next we want to map each rating to a 1, because we will divide this
    //by total rating because (idavg = totalRating/numRatings)
    val assoc1PerRating = idStarsRDD
      .mapValues((_, 1))
    //RDD: (business_id, (stars, 1)) : (String, (Double, Int))

    //now we want to reduce upon all ratings, and there associated count, keep ID
    //we want to find the average rating that users are giving reviews
    val totalsForRatingAndStarsPerUser = assoc1PerRating
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    //RDD: (business_id, (totalStars, totalRatings)) : (String, (Double, Int))

    val mapAverageRatingsToUser = totalsForRatingAndStarsPerUser
      .mapValues(x => x._1 / x._2)
      .filter(_._2 > 3)

    val averagesDF = mapAverageRatingsToUser
      .toDF("business_id", "stars")

    //this file we just practice joining between 2 RDDs
    //Note we had to convert from dataframes to RDD to perform
    //reduceByKey and mapping objects
    businessDF.join(averagesDF,
      businessDF.col("business_id") === averagesDF.col("business_id"))
      .select("name", "stars")
      .where("name like 'M%'")
      .show(10)

    readLine
    spark
      .stop
  }
}
