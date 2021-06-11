/*
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/** Listens to a stream of Tweets and keeps track of the most popular
  * hashtags over a 5 minute window.
  *  1. First, we want to stream most popular words
  *     2. Next, find average tweet length!
  *
  *
  * HEY: the way I did part 3 for AVG tweet length was much more
  * inefficient than using foreachRDD from course 2 SS with DStreams
  */

object PopularHashtagsDStreams {

  //takes a tweet, and removes all the hashtags so we can judge avg length
  //of a tweet
  def removeBadData(tweet: String): String = {
    //split everything, delete # starting strings, convert array back to string
    val message = tweet
      .split(" ")
      //consider only english words, dont take hashtags or @users
      .filterNot(x => x.contains("#") || x.contains("@") || x.contains("RT"))
      .filter(_.matches("[a-zA-Z]+"))
      //makes this a string with a space appended to it. How convenient.
      .mkString(" ")

    message
  }

  //both these filter depending on if we want popular tags, or words
  def filterWords(word: String): Boolean = {
    val bannedWords = List("rt", "", "a", "the", "to", "de", "and", "que", "i",
      "of", "you", "in", "is", "me", "this", "for", "on", "la", "no")

    !word.contains("#") &&
      !bannedWords.contains(word.toLowerCase)
  }

  def startsWithSharp(word: String): Boolean = {
    word.contains("#")
  }

  /** Configures Twitter service credentials using twiter.outFile in the main workspace directory */
  def setupTwitter() = {
    import scala.io.Source

    for (line <- Source.fromFile("src/main/resources/RDDFiles/twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    Logger
      .getLogger("org")
      .setLevel(Level.ERROR)

    // Configure Twitter credentials using twitter.txt
    setupTwitter

    // Set up a Spark streaming context named "PopularHashtags" that runs locally using
    // all CPU cores and two-second batches of data
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(2))

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils
      .createStream(ssc, None)

    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets
      .map(_.getText)

    // Blow out each word into a new DStream
    val tweetwords = statuses
      .flatMap(_.split(" "))


    //PART 1: most popular hashtag

    //eliminate anything that is a hashtag (words only)
    val wordsNoTags = tweetwords
      .filter(filterWords)

    //map each word like we did in word counts
    val wordsKeyValues = wordsNoTags
      .map((_, 1))

    //now we reduce byKeyAndWindow (same as reduceByKey just specify a window time)
    val popWordsCounts = wordsKeyValues
      //the _-_ is merely an optimization to make reduceByKeyAndWindow efficient
      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(2))

    val sortedWordCounts = popWordsCounts
      //we want it descending, which makes largest print on top, first
      .transform(_.sortBy(_._2, false))

    sortedWordCounts
    // .print

    //print the top 10, or send in a parameter to print more
    sortedWordCounts
      .saveAsTextFiles("src/main/resources/RDDFiles/outfiles/textfiledata/text.txt")

    //PART 2: most popular word

    // Now eliminate anything that's not a hashtag
    val hashtags = tweetwords
      .filter(startsWithSharp)

    // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
    val hashtagKeyValues = hashtags
      .map((_, 1))

    // Now count them up over a 5 minute window sliding every one second
    val hashtagCounts = hashtagKeyValues
      .reduceByKeyAndWindow(
        _ + _,
        _ - _,
        Seconds(300),
        Seconds(2))

    // Sort the results by the count values (each RDD gets a _.sortBy()
    val sortedResults = hashtagCounts
      .transform(_.sortBy(_._2, false))

    val englishSortedResults = sortedResults
      .map(x => x._1)
      .map(removeBadData)

    // Print the top 10
    englishSortedResults
      .print

    //PART 3: average length per tweet

    val allSentences = statuses
      .map(removeBadData)

    //get rid of the tweets that ar empty
    val sentences = allSentences
      .filter(_.length > 0)

    sentences
      .print

    //map every tweet to its length DStream: (Int, Int)
    val lengthsOFTweets = sentences
      .map(x => (x.length, 1))

    //print lnegths with 1
    lengthsOFTweets
    //.print

    //add everything together
    //This is the same operation we did, .reduce, it just tells the DStream to
    //toss data after 5 minutes, and keep new data, calculate it into our DStream
    //every second, meaning both values continue to grow
    //This is why we saw our reduce addition get very large
    val totalTweetCharsWithCount = lengthsOFTweets
      .reduceByWindow((x, y) =>
        //param1 is the reduceOP
        (x._1 + y._1, x._2 + y._2),
        //param2: to take off data after 300 seconds
        (x, y) => (x._1 - y._1, x._2 - y._2),
        //window time frame
        Seconds(300),
        //how often we update our data structure
        Seconds(4)
      )
      //so we don't recalculate the DAG
      .cache

    //get the average
    val averageTweetLength = totalTweetCharsWithCount
      .map(x => x._1 / x._2)

    averageTweetLength
      .print

    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!
    ssc
      .checkpoint("src/main/resources/RDDFiles/outfiles/streamdata/data")
    ssc
      .start
    ssc
      .awaitTermination
  }
}
*/
