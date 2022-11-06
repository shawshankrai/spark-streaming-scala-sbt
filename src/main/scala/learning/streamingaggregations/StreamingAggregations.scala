package learning.streamingaggregations

import org.apache.spark.sql.streaming.OutputMode.Complete
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.util.concurrent.TimeUnit
object StreamingAggregations {

  val spark: SparkSession = SparkSession.builder()
    .appName("Stream-Agg")
    .master("local[2]")
    .getOrCreate()
  import spark.implicits._

  def distinct(): Unit = {
    // distinct is not supported, Spark have to keep everything in memory to provide this functionality
  }

  def numericalAggregation(aggFunction: Column => Column): Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val numbers = lines.select($"value" cast "integer" as "number")
    val aggDataFrame = numbers.select(aggFunction($"number"))

    aggDataFrame.writeStream
      .format("console")
      .outputMode(Complete)
      .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
      .start()
      .awaitTermination()

    /**
     * -------------------------------------------
     * Batch: 1
     * -------------------------------------------
     * +-----------+
     * |sum(number)|
     * +-----------+
     * |         55|
     * +-----------+
     * */
  }

  def groupWords(): Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val wordCount: DataFrame = lines.select($"value"  as "word").groupBy("word").count()

    wordCount.writeStream
      .format("console")
      .outputMode(Complete)
      .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
      .start()
      .awaitTermination()

    /**
     * -------------------------------------------
     * Batch: 2
     * -------------------------------------------
     * +----+-----+
     * |word|count|
     * +----+-----+
     * | efg|    2|
     * | trs|    2|
     * | rst|    1|
     * | abc|    3|
     * +----+-----+
     * */
  }

  def main(args: Array[String]): Unit = {
    // StreamLineCount
    // numericalAggregation(sum)
    // multiple aggregations are not supported, sorting and distinct is not supported
    groupWords()
  }

}
