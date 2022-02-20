package scrm.practical.ex2

import org.apache.spark.sql.functions

class SparkChallengeTest extends SparkTestBase {

  "transformation" should "retrieve 313 time slots and top 10" in {
    // Given
    val testData = spark
      .read
      .option("header", true)
      .csv("src/test/resources/events.csv")
      .withColumn("time", functions.to_timestamp(functions.column("time")))

    // When
    val result = SparkChallenge.transformation(testData)

    // Then
    /* Expected top 10 elements
    (2016-07-26 15:10:00.0,2016-07-26 15:20:00.0,185)
    (2016-07-27 10:00:00.0,2016-07-27 10:10:00.0,184)
    (2016-07-27 15:50:00.0,2016-07-27 16:00:00.0,184)
    (2016-07-26 19:00:00.0,2016-07-26 19:10:00.0,184)
    (2016-07-27 02:40:00.0,2016-07-27 02:50:00.0,184)
    (2016-07-27 03:20:00.0,2016-07-27 03:30:00.0,184)
    (2016-07-27 08:20:00.0,2016-07-27 08:30:00.0,182)
    (2016-07-27 01:50:00.0,2016-07-27 02:00:00.0,182)
    (2016-07-27 09:30:00.0,2016-07-27 09:40:00.0,182)
    (2016-07-27 00:40:00.0,2016-07-27 00:50:00.0,180)
     */
    result._2.length should be(10)
    result._2.map(_._3).toList should equal(Seq(185, 184, 184, 184, 184, 184, 182, 182, 182, 180))

    // expected df elements
    result._1.count() should be(313)
    val mappedDfResults = result._1
      .collect()
      .map(row => (row.getTimestamp(0).toString, row.getTimestamp(1).toString, row.getAs[Double](2), row.getAs[Double](3)))
      .toSeq

    // top 4 records
    mappedDfResults.contains(("2016-07-25 21:40:00.0", "2016-07-25 21:50:00.0", 6.4, null)) should be(true)
    mappedDfResults.contains(("2016-07-25 21:50:00.0", "2016-07-25 22:00:00.0", 14.7, 1.2222222222222223)) should be(true)
    mappedDfResults.contains(("2016-07-25 22:00:00.0", "2016-07-25 22:10:00.0", 16.2, 2.111111111111111)) should be(true)
    mappedDfResults.contains(("2016-07-25 22:10:00.0", "2016-07-25 22:20:00.0", 16.9, 4.666666666666667)) should be(true)

    // empty record for reference
    mappedDfResults.contains(("", "", 0.0, 0.0)) should be(false)
  }

}
