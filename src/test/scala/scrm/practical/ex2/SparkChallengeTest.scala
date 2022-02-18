package scrm.practical.ex2

import org.mockito.ArgumentMatchers._
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest._
import flatspec._
import matchers._
import org.apache.spark.sql.SparkSession
import scrm.practical.ex2.SparkChallenge.transformation

class SparkChallengeTest extends AnyFlatSpec with should.Matchers with MockitoSugar {

  val spark = SparkSession.builder
    .master("local[*]")
    .getOrCreate()

  // read subset of data for testing
  val testData = spark
    .read
    .option("header", true)
    .csv("src/test/resources/ex2/events.csv")


  "TimeWindowTransformation" should "retrieve n time slots" in {
    ???
  }

}
