import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql._
import SparkHelper.RDDExtensions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

object FilterLogsSimilarError {

  //carried over from FilterLogs.scala...
  // useful for extracting timestamp to reduce on for memcheck
  //we made this because we needed to filter out milliseconds for reduce OP
  def timeMapper(line: String): (String, String) = {
    //returns tuple ( time, data)
    (("([0-9]{2})" +
      "(:[0-9]{2})" +
      "(:[0-9]{2})?")
      .r
      .findFirstIn(line)
      //Option[String] requires this to get the String literal
      .getOrElse(0)
      .toString, line)
  }

  //memcheck mapper only
  def mapperMemCheck(line:String) = {
    val parsedLine = line.split("\\s+")
    var stamp = ""
    if (parsedLine.size > 3) {
      stamp = parsedLine(1)
    }
    stamp + " " + line
      .split("\\s+")
      .drop(5)
      .mkString(" ")
  }

  //memcheck
  def filterMemCheck(line: String): Boolean = {
    val parsedLine = line.toLowerCase.split("\\W")
    line.contains("SystemMonitor")
  }

  //for normal check, the numbers are based on columns in log files
  def mapper(line: String): String = {
    val parsedLine = line.split("\\s+")
    var stamp = ""
    if (parsedLine.size > 3) {
      stamp = parsedLine(1)
    }
    stamp + "> " + line
      .split("\\s+")
      .drop(6)
      .mkString(" ")
  }

  def filter(line: String): Boolean = {
    //split the string into an array of only words
    val parsedLine = line.toLowerCase.split("\\W")
    //keep lines of certain length, or if they contain error or warning
    line.length < 210 &&
      (parsedLine.contains("error")
        || parsedLine.contains("warning")) &&
      !parsedLine.contains("info")
  }

  def main(args: Array[String]): Unit = {

    //query for these list elements
    val listOfSoakTestBugs = List(
      "I cast_shell: Chirp [*:*:WARNING:render_delay_buffer.cc(*)] Render buffer",
      "I cast_shell: Chirp [*:*:WARNING:gdc.cc(*)] No free output buffer available!",
      "I cast_shell: [*:*:WARNING:audio_input_delegate.cc(*)] [ALIGN] Input overrun",
      "ERROR:mesh_cast_receiver_observer.cc(*)] Not implemented reached in virtual void chromecast::dbus_mojo_adaptor::MeshCastReceiverObserver::OnTimeFormatChanged(chromecast::mojom::SetupTimeFormat)",
      "[ERROR:hal_utils.cc(*)] Unable to get interface config on uap0 from ap-hal, or hal response does not contain interface config"
    )

    //name of giant log file you want to look into
    val inFile = "testiterationendlogs.txt"

    //name your outputFiles after xforms and action
    val outFile1 = "soakStructLogs.txt"
    val outFile2 = "soakFilteredSqlLogs.txt"
    val outFile3 = "soakSysMemLogs.txt"

    //for Spark SQL query %data%data%data%
    //note: if you put mom(free) here, this will not show since in our filter above, it
    //will automatically remove this (this program was written in a day its shit)
    val linePiece1 = "I DeviceMsg: /dev/kmsg"
    val linePiece2 = "isp_v4l2_notify_event@isp-v4l2.c"
    val linePiece3 = "GENERIC(ERR) :Error, no fh_ptr exists"

    //set up
    Logger
      .getLogger("org")
      .setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("FilterLogsSimilarErrors")
      .master("local[8]")
      .getOrCreate

    //map to shrink each line, readability
    val logFileRDD = spark
      .sparkContext
      .textFile(s"src/main/resources/logFiles/$inFile")
      .map(mapper)

    //filter out irrelevant lines
    val structuredLogData = logFileRDD
      .filter(filter)

    //make a DF for simple sql query out lines we want to manage
    import spark.implicits._
    val filteredLogsDF = structuredLogData
      .toDF("lines")

    //make sql view to query for the line we want to see
    filteredLogsDF
      .createOrReplaceTempView("data")

    //now here is where Spark SQL is much more powerful than Spark Map/Filter/Reduce
    val filteredLogsSql = spark
      .sql(s"Select lines from data where lines like '%$linePiece1%$linePiece2%$linePiece3%'")
      .as[String]
      .rdd

    //TODO: Next 4 vals only work with System Monitor labelled lines in logfile
    //Attempt to get memory leaks monitored throughout the program
    //map to shrink length of each line
    val logFileRDD2 = spark
      .sparkContext
      .textFile(s"src/main/resources/logFiles/$inFile")
      .map(mapperMemCheck)

    //check mem before and after
    val filterLogsMemCheck = logFileRDD2
      .filter(filterMemCheck)

    //pull millisecconds out (only for the systemmonitor mem free check, comment out
    val filteredLogsSqlMappedTuple = filterLogsMemCheck
      .map(timeMapper)

    //reduce on 6digtime 00:00:00 (ie. treats all millis the same (time, data)
    val reducedLogsForSysMonitor = filteredLogsSqlMappedTuple
      .reduceByKey((x, y) => x)
      .map(_._2)


    //print out the number of timeszzzz the line occurred
    println("\n" + filteredLogsSql.count + s" line occurrences of $linePiece1$linePiece2$linePiece3")

    //save RDD data for inspection (structuredLogData, filteredLogsSql, or reducedLogsForSysMonitor)
    structuredLogData
      .saveAsSingleTextFile(s"src/main/resources/logFiles/outFiles/$outFile1")

    //filter for the lines we want to see and count in general
    filteredLogsSql
      .saveAsSingleTextFile(s"src/main/resources/logFiles/outFiles/$outFile2")

    //filter for just system momitor logs, but same idea as above
    reducedLogsForSysMonitor
      .saveAsSingleTextFile(s"src/main/resources/logFiles/outFiles/$outFile3")

    readLine
    spark
      .stop
  }
}
