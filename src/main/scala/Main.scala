import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions._

import scala.io.Source

class ImdbReport(spark: SparkSession) {
  import spark.sqlContext.implicits._

  def topMovies(
      ratings: Dataset[Rating],
      n: Int = 20,
      minNumVotes: Int = 50
  ): Dataset[String] = {
    val avgNumVotes =
      ratings.agg(avg($"numVotes")).as[Double].collect().headOption.getOrElse(1)
    ratings
      .toDF()
      .select(
        $"tconst",
        (($"numVotes" / avgNumVotes) * $"avgRating").as("rating"),
        $"numVotes"
      )
      .filter($"numVotes" >= minNumVotes)
      .sort($"rating".desc)
      .limit(n)
      .select($"tconst".as[String])
  }

  def report(
      movies: Dataset[String],
      titleBasics: Dataset[TitleBasics],
      nameBasics: Dataset[NameBasics]
  ) =
    movies
      .join(titleBasics, "tconst")
      .join(nameBasics, array_contains($"knownFor", movies("tconst")), "left")
      .groupBy(movies("tconst"))
      .agg(
        collect_set(nameBasics("name")).as("creditedPersons"),
        collect_set(titleBasics("title").as("Titles"))
      )

}

case class Rating(tconst: String, avgRating: Double, numVotes: Int)
case class TitleBasics(tconst: String, title: String)
case class NameBasics(nconst: String, name: String, knownFor: Array[String])

object Main extends App {

  val dataPathStr = args.headOption.getOrElse(
    throw new Exception("Pls pass the path to the data as the fist argument")
  )

  val ratingsFile = s"$dataPathStr/title.ratings.tsv"
  val namesBasicsFile = s"$dataPathStr/name.basics.tsv"
  val titlesBasicsFile = s"$dataPathStr/title.basics.tsv"

  val sess =
    SparkSession.builder().appName("Test").master("local[*]").getOrCreate()

  import sess.implicits._

  def readTsv(path: String) =
    sess.read
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .option("header", "true")
      .csv(path)

  val ratingsDs = readTsv(ratingsFile)
    .withColumnRenamed("averageRating", "avgRating")
    .as[Rating]
  val namesDs = readTsv(namesBasicsFile)
    .select(
      $"nconst",
      $"primaryName".as("name"),
      split($"knownForTitles", ",").as("knownFor")
    )
    .as[NameBasics]
  val titlesDs = readTsv(titlesBasicsFile)
    .select($"tconst", $"originalTitle".as("title"))
    .as[TitleBasics]

  val rep = new ImdbReport(sess)

  rep.report(rep.topMovies(ratingsDs), titlesDs, namesDs).show()

  sess.stop()
}
