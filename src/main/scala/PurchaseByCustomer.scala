import org.apache.log4j._
import org.apache.spark._


object PurchaseByCustomer {

  //note the type conversions
  def parseLine(line: String): (Int, Float) = {
    val list = line.split(",")
    val custId = list(0).toInt
    val purchaseAmt = list(2).toFloat
    (custId, purchaseAmt)
  }

  def main(args: Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCount")

    // Read each line of my book into an RDD, user picks file at cmd line
    val textFile = args(0) //access lists data (aka array)
    val input = sc.textFile(s"src/main/resources/RDDFiles/$textFile")

    //write simple script that adds up the amount spent by each customer
    //the customer ID is field 0 the price is field 2,

    //keep only customerId and Purchase Amt (id, purchase)
    val parsedLines = input
      .map(parseLine)

    //add all values with similar keys (id, purchaseTotal)
    val purchaseTotals = parsedLines
      .reduceByKey((x,y) => x+y)

    //sort by the purchase Amt, element 2 (id, purchaseTotal)
    val sortedByMostSpent = purchaseTotals
      .sortBy(_._2, false)

    //pull into memory, but only a select few
    val collectedData = sortedByMostSpent
      .take(20)

    collectedData.foreach(println)

  }

}
