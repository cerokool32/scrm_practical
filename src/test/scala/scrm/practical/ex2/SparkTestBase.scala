package scrm.practical.ex2

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

abstract class SparkTestBase extends AnyFlatSpec with should.Matchers with BeforeAndAfterAll {

  @transient implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder
      .config("spark.sql.session.timeZone", "UTC")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }
}
