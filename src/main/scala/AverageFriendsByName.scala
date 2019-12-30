import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}



object AverageFriendsByName {
  //a predicate to be used on each key of the rdd
  def parseLine(line: String):(String, Int) = {
    //seems to be very common
    val fields = line.split(",")
    val name = fields(1)
    //because split breaks a line into 4 strings toInt will be common
    val friends = fields(3).toInt
    (name, friends)
  }


  def main(args: Array[String]): Unit = {
    //enums are caps!
    Logger.getLogger("org").setLevel(Level.ERROR)
    //local is lowercase l. damn that is particular as hell
    val sc = new SparkContext("local[*]", "FriendsByName")

    val fakeFriendsRDD = sc.textFile("src/main/resources/RDDFiles/fakefriends.csv")
    //pass each line to predicate parseLine returning tuple of data we want
    val friendsCounterRDD = fakeFriendsRDD
      .map(x => parseLine(x))

    //(name, friends), give each record (row), map its value to 1
    //this is mostly done best for counting purposes
    val assocOneToFriends = friendsCounterRDD
      .mapValues(x => (x, 1))

    //we have an RDD of the form (name, (friends, 1))
    //we should now combine all the friends, and names, then divide by key
    val combineNamesValuesRDD = assocOneToFriends
      .reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2))


    combineNamesValuesRDD
      .take(5)
      .foreach(println)

    //we still of the form (name, (friends, x)) but we really want to
    //map the average of (friends / countNames) so we map again?
    val averageFriendsByName = combineNamesValuesRDD
      //basically for each tuple, divide and map to 1 final value to the name
      .mapValues(x => x._1 / x._2)

    val collectedData = averageFriendsByName
      .collect

    collectedData.foreach(println)
  }
}
