import java.nio.charset.CodingErrorAction
import org.apache.log4j._
import org.apache.spark._
import scala.io.{Codec, Source}

object PopularSuperHeroes {

  //now this is a snazy function!
  def parseLine(line: String): (Int, Int) = {
    //split on any white spaces
    val fields = line.split("\\s")
    (fields(0).toInt, fields.length - 1)
  }

  //method to load superfriends into associative array (map)
  def loadSuperFriends(): Map[Int, String] = {
    val fileName = "Marvel-names.outFile"
    // Handle character encoding issues for when on cluster:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    //init our Map
    var superHeroesMap: Map[Int, String] = Map()
    //grab file data
    val superHeroDataLines = Source.fromFile(s"src/main/resources/RDDFiles/$fileName").getLines()
    //loop through lines, keep what we need
    for (line <- superHeroDataLines) {
      val fields = line.split("\"")
      if (fields.length > 1) {
        superHeroesMap += (fields(0).trim.toInt -> fields(1))
      }
    }
    //map of the form (heroId(Int), hero Name(String))
    superHeroesMap
  }


  def main(args: Array[String]) = {

    Logger
      .getLogger("org")
      .setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "PopularSuperHeroes")

    //We've loaded the data, now we need to broadcast to each Node
    //map of the form (heroId(Int), hero Name(String))
    val superHeroMapCast = sc.broadcast(loadSuperFriends)
    //load our textFile superhero graph
    val superHeroGraph = sc.textFile("src/main/resources/RDDFiles/Marvel-graph.outFile")

    //first, lets count each line, associate first val as key, second as string
    //tuple is an RDD of (Strings)
    val heroFriends = superHeroGraph
      .map(parseLine)

    //tuple is of the form (heroId(Int), numFriends(Int))
    val heroOccurancesByKey = heroFriends
      .reduceByKey((x, y) => (x + y))

    //tuple of form (heroId(Int), numFriends(Int)) reduced
    val heroWithMaxFriends = heroOccurancesByKey
      .sortBy(_._2, false)

    //weave into the broadcasted map to get resulting hero, call take here
    val mostPopularHero = heroWithMaxFriends
      .map(x => (superHeroMapCast.value(x._1), x._2))
      .first

    //print to see if working, then move to mapping over to winner
    val mostPopName = mostPopularHero._1
    val mostPopNumFriends = mostPopularHero._2
    println(s"The most popular Hero is: $mostPopName with $mostPopNumFriends friends!")

  }
}
