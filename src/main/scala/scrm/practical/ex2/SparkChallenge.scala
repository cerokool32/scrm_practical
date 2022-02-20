package scrm.practical.ex2

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object SparkChallenge {

  def transformation(input: DataFrame)(implicit spark: SparkSession): (Dataset[Row], Array[Row]) = {
    import spark.implicits._
    val frame1DF = input
      .withColumn("action_n", lit(1))
      .groupBy(window($"time", "1 minutes"))
      .pivot("action", Seq("Open", "Close"))
      .agg(
        sum($"action_n")
      )
      .withColumnRenamed("Open", "open_1")
      .withColumnRenamed("Close", "close_1")
      .select("window","window.start", "window.end", "open_1", "close_1")
      .orderBy("window")

    val frame1To10DF = frame1DF
      .groupBy(window($"window.start", "10 minutes"))
      .agg(
        avg("open_1").as("open_10_avg"),
        avg("close_1").as("close_10_avg"),
      )
      .orderBy("window")

    val top10Records = input
      .withColumn("action_n", lit(1))
      .groupBy(window($"time", "10 minutes"))
      .pivot("action", Seq("Open"))
      .agg(
        sum($"action_n")
      )
      .select("window","Open")
      .orderBy(desc_nulls_last("Open"))
      .head(10)

    (frame1To10DF, top10Records)
  }

}
