import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll
import org.apache.spark.sql.SparkSession

class MainSpec extends Specification with AfterAll {

  "topMovies" >> {
    "gets the ratings right" >> { testTopMoviesRatings() }
    "filters out rows with less than N wotes" >> { testTopMoviesWOtesFilter() }
  }
  "Report aggregates the data" >> { testFinalReport() }

  val sess =
    SparkSession.builder().appName("Test").master("local").getOrCreate()
  val r = new ImdbReport(sess)
  import r._

  import sess.implicits._

  def testTopMoviesRatings() = {
    val data = List(Rating("1", 3.5, 55), Rating("2", 2, 50))
    topMovies(data.toDS(), 1, 1)
      .collect()
      .toList must contain(exactly("1"))
  }

  def testTopMoviesWOtesFilter() = {
    val data = List(Rating("1", 3.5, 20), Rating("2", 2, 50))
    topMovies(data.toDS(), 1, 30)
      .collect()
      .toList must contain(exactly("2"))
  }

  def testFinalReport() = {
    val movies = List("1").toDF("tconst").as[String]
    val titles = List(
      TitleBasics("1", "T1.1"),
      TitleBasics("1", "T1.2"),
      TitleBasics("2", "T2")
    ).toDS()
    val names =
      List(
        NameBasics("", "Actor 1", Array("2", "1", "3")),
        NameBasics("", "Actor 2", Array("2", "3"))
      ).toDS()

    val resRow = report(movies, titles, names).collect().toList

    resRow must have size (1)

    resRow.head.getAs[String]("tconst") must be_==("1")

    resRow.head.getSeq[String](1).toList must contain(
      exactly("Actor 1")
    )

    resRow.head.getSeq[String](2).toList must contain(
      exactly("T1.1", "T1.2")
    )
  }

  def afterAll() = sess.close()

}
