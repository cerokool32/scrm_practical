package scrm.practical.ex2

import org.mockito.ArgumentMatchers._
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest._
import flatspec._
import matchers._
import org.apache.spark.sql.{SparkSession, functions}
import scrm.practical.ex2.SparkChallenge.transformation

class SparkChallengeTest extends AnyFlatSpec with should.Matchers with MockitoSugar {

  implicit val spark = SparkSession.builder
    .config("spark.sql.session.timeZone","UTC")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // read subset of data for testing
  val testData = spark
    .read
    .option("header", true)
    .csv("src/test/resources/events.csv")
    .withColumn("time", functions.to_timestamp($"time"))


  "TimeWindowTransformation" should "retrieve n time slots" in {
    val result = SparkChallenge.transformation(testData)

    ???
  }

}
