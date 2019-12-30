import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql._
import SparkHelper.RDDExtensions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

//great utility class I will use for much more than just logs
//write program to filter out the [Testbed.... arbitrary data]
//note 1. also, we can only call DS on an RDD if it contains objects
//2. Can Auto structure json much easier with .as[] and .toDS
//To be honest, we really just want to work with json
//Lesson learned. If cannot figure out the logic behind some new
//method, should definitely hard code it instead of being super tired
//and wasting time...
object FilterLogsTwoFiles {

  final case class BigLogsData(timeStamp: String,
                               dataAtTimeStamp: String)

  //if we match hour and minute, our where clause will keep 60 seconds
  //of data
  def grabTimeFromString(line: String): String = {
    ("([0-9]{2})" +
      "(:[0-9]{2})" +
      "(:[0-9]{2})?")
      .r
      .findFirstIn(line)
      //Option[String] requires this to get the String literal
      .getOrElse(0)
      .toString
  }

  //create structure out of the log file, something we can deal with
  def objectMapperForBigLogs(line: String): BigLogsData = {
    //grap time stamp from our line, make it our first element
    val time = grabTimeFromString(line)
    //handle the Some, None, Option syntax
    BigLogsData(time, line)
  }

  def keepTimeAndDataFromBigFile(line: String): String = {
    val parsedLine = line.split("\\s+")
    val stamp = parsedLine(1).trim
    //need a String data structure (like only time ever use var)
    var data = stamp + " "
    for (piece <- 0 to parsedLine.size - 1) {
      //grab only the data that matters, later columns, first 8 data is bad per line
      if (piece > 8) {
        data += parsedLine(piece) + " "
      }
    }
    data
  }

  def main(args: Array[String]): Unit = {

    val timeStampAtLine = 103
    val inFile = "S_FT.txt"
    val inFile2 = "big_S_FT.txt"
    val outFile = "outS_FT.txt"
    val outFile2 = "outbig_S_FT"

    //parameters to grab from log
    val extensions =
      List(
        "reboot",
        "error",
        "reboots",
        "total reboots",
        "failed",
        "traceback"
      )

    Logger
      .getLogger("org")
      .setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("FilterLogs")
      .master("local[8]")
      .getOrCreate

    //to utilize datasets
    import spark.implicits._



/*    val h = upper("Hello")
    //this essentially makes it so we can create functions that
    //operate on an entire column, ie all data points
    val upperUDF = udf(upper)

    List((0, "hello"), (1, "world"))
      .toDF("id", "text")
      .withColumn("upper", upperUDF(col("text")))
      .show()*/

    val smallLogsRDD = spark
      .sparkContext
      .textFile(s"src/main/resources/logFiles/$inFile")

    //literally just a filter to keep lines with error since I made it flat
    //study this line later, cool stuff very functional
    //regex for a word char is 'w', and non-word char is 'W'
    val filteredRDD = smallLogsRDD
      .filter(x => extensions
        .exists(e => x
          .toLowerCase
          .replaceAll("\\W", " ")
          .split("\\s+")
          .contains(e)))
      .cache

    //see SparkHelper class for why this is awesome
    filteredRDD
      .saveAsSingleTextFile(s"src/main/resources/outFiles/$outFile")

    //Grab a time stamp given our filtered RDD of
    // what we wanted in the program parameters from a specific String
    // in the RDD this to be dynamic (not hard coded)
    val aTimeStamp = filteredRDD
      .zipWithIndex
      //starts at 0so- 1, these next two are what grab the specific line
      .filter(_._2 == (timeStampAtLine.toInt - 1))
      .map(_._1)
      .first
      .split("\\s+")(2)

    //use this as key, add 1 minute to it, and print results out of
    //mappedRDD
    val sixDigTime = grabTimeFromString(aTimeStamp)

    //take our other RDD, and search for the line with this String.. easy
    val bigLogsRDD = spark
      .sparkContext
      .textFile(s"src/main/resources/logFiles/$inFile2")

    //BigLogsData is (String, String), (timeStamp, Data)
    //we will use the timestamp from aTimeStamp
    // to key into the key on this file
    val bigLogsDS = bigLogsRDD
      .map(objectMapperForBigLogs)
      .toDS
      .cache

    //lets convert time stamp to a hashcode, and return only that
    //specific hashcode since itll only match one time
    val theDataDS = bigLogsDS
      .select("TimeStamp", "DataAtTimeStamp")
      .withColumn("TimeStamp", (col("TimeStamp").cast("timestamp")))
      //grab the lines from the DF that we want to look through
    //now we cast the column to a real Timestamp

    //lets make a view we can sql query easier
    theDataDS
      .createOrReplaceTempView("Log_data")

    val theDataInTime = spark
      .sql("Select DataAtTimeStamp from Log_data" +
        s" where TimeStamp = '$sixDigTime'")

    //Now make it an RDD so we can keep the data we want
    val theDataRDD = theDataInTime
      .as[String]
      .rdd
      .map(keepTimeAndDataFromBigFile)

    theDataInTime
      .show(5)

    //we Have the exact data we want to display, now we need the data
    //within 30 seconds of our time stamp
    //I'm thinking we need to either define a method for this,
    //or maybe there's some crazy sql query we can do
    //we're getting very close.. Utilizing everything I've learned so
    //far from the course. Once I finish this I can move onto MLLib
    theDataRDD
      .saveAsSingleTextFile(s"src/main/resources/outFiles/$outFile2")

    readLine
    spark
      .stop
  }
}
