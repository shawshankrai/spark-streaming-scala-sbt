package learning.structuredstreaming

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import common._
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration.DurationInt

object StreamingDataFrames {

  private val spark = SparkSession.builder()
    .appName("Streaming data frames")
    .master("local[2]") // at least 2 threads
    .getOrCreate()
  import spark.implicits._

  def readFromSocket(): Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // transformation
    val shortLines: DataFrame = lines.filter(functions.length($"value") <= 5)
    println(shortLines.isStreaming)

    // query to be run on lines streaming dataframe
    val query = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start() // Start is Asynchronous, will not wait

    // Awaiting for query termination
    query.awaitTermination()
  }

  def readFromFiles(): Unit = {
    val stocksDF = spark.readStream.format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema)
      .load("src/main/resources/data/stocks") // monitors files changes

    // query to be run on lines streaming dataframe
    val query = stocksDF.writeStream
      .format("console")
      .outputMode("append") // default output mode
      .start() // Start is Asynchronous, will not wait

    // Awaiting for query termination
    query.awaitTermination()
  }

  def demoTriggers(): Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // query to be run on lines streaming dataframe
    val query = lines.writeStream
      .format("console")
      .outputMode("append")
       //.trigger(Trigger.ProcessingTime(2.seconds)) // batch runs every two seconds
       //A trigger that process only one batch of data in a streaming query then terminates the query
       //.trigger(Trigger.Once())
       // A trigger that continuously processes streaming data, asynchronously checkpointing at the specified interval
      .trigger(Trigger.Continuous(2.seconds))
      .start() // Start is Asynchronous, will not wait

    // Awaiting for query termination
    query.awaitTermination()
  }
  def main(args: Array[String]): Unit = {
    demoTriggers()
  }
}
