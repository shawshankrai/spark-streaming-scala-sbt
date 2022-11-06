package learning.streamingjoins

import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.OutputMode.Append
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object StreamingJoins {

  /**
   * local[*] Run Spark locally with as many worker threads as logical cores on your machine.
   * */
  val spark: SparkSession = SparkSession.builder()
    .appName("Streaming-joins-1")
    .master("local[2]")
    .getOrCreate()


  import spark.implicits._

  val guitarPlayers: DataFrame = spark.read.json("src/main/resources/data/guitarPlayers/guitarPlayers.json")
  val guitars: DataFrame = spark.read.json("src/main/resources/data/guitars/guitars.json")
  val bands: DataFrame = spark.read.json("src/main/resources/data/bands/bands.json")

  private val joinCondition: Column = guitarPlayers.col("band") === bands.col("id")
  val guitaristBands: DataFrame = guitarPlayers.join(bands, joinCondition, "inner")

  def joinStreamWithStatic(): Unit = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    /** Parses a column containing a JSON string into a StructType with the specified schema */
    val streamBandsDataframe = lines
      .select(from_json($"value", bands.schema) as "band") // returns complex structure
      .selectExpr("band.id as id",
        "band.name as name",
        "band.hometown as hometown",
        "band.year as year")

    val streamBandGuitaristDataframe = streamBandsDataframe.join(guitarPlayers,
      guitarPlayers.col("band") === streamBandsDataframe.col("id"),
      "inner")

    streamBandGuitaristDataframe.writeStream
      .format("console")
      .outputMode(Append)
      .trigger(ProcessingTime("2 seconds"))
      .start()
      .awaitTermination()

    /**
     * -------------------------------------------
     * Batch: 1
     * -------------------------------------------
     * +---+------------+-----------+----+----+-------+---+------------+
     * | id|        name|   hometown|year|band|guitars| id|        name|
     * +---+------------+-----------+----+----+-------+---+------------+
     * |  1|       AC/DC|     Sydney|1973|   1|    [1]|  1| Angus Young|
     * |  3|   Metallica|Los Angeles|1981|   3|    [3]|  3|Kirk Hammett|
     * |  0|Led Zeppelin|     London|1968|   0|    [0]|  0|  Jimmy Page|
     * +---+------------+-----------+----+----+-------+---+------------+
     * */

    /**
     * Stream - Static: Inner, Left Outer Supported, not stateful,
     * Right Outer, Full Outer Not supported,
     * Left Semi Supported, not stateful
     *
     * Static - Stream: Inner, Right Outer Supported, not stateful
     * Left Outer, Full Outer, Left Semi Not supported
     *
     * Joining of static DF to stream DF is not supported in case when,
     * spark have to keep whole streaming DF in memory to join it with static DF
     *
     * for Example: Left join between Static and stream will require
     * all rows from static to be present with all matching row of stream
     * since join happens in micro batch it is not possible to get older streaming data
     * and return all matching rows
     *
     * Wherever joining is possible it will be taking stream as base and it will be not stateful
     * Same issue will arise if join needs to be state full, we have to keep whole stream in memory
     * */
  }

  def joinStreamWithStream(): Unit = {
    val streamBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json($"value", bands.schema) as "band") // returns complex structure
      .selectExpr("band.id as bandId",
        "band.name as name",
        "band.hometown as hometown",
        "band.year as year")

    val streamGuitaristsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .select(from_json($"value", guitarPlayers.schema) as "player") // returns complex structure
      .selectExpr("player.id as playerId",
        "player.name as name",
        "player.guitars as guitars",
        "player.band as bandID")

    val streamedJoin = streamGuitaristsDF.join(streamBandsDF,
      streamGuitaristsDF.col("bandID") === streamBandsDF.col("bandId"),
      "inner")

    streamedJoin.writeStream
      .format("console")
      .outputMode(Append)
      .trigger(ProcessingTime("2 seconds"))
      .start()
      .awaitTermination()

    /**
     * +--------+------------+-------+------+------+------------+-----------+----+
     * |playerId|        name|guitars|bandID|bandId|        name|   hometown|year|
     * +--------+------------+-------+------+------+------------+-----------+----+
     * |       0|  Jimmy Page|    [0]|     0|     0|Led Zeppelin|     London|1968|
     * |       1| Angus Young|    [1]|     1|     1|       AC/DC|     Sydney|1973|
     * |       3|Kirk Hammett|    [3]|     3|     3|   Metallica|Los Angeles|1981|
     * +--------+------------+-------+------+------+------------+-----------+----+
     * */

    /**
     * Stream - Stream: 	Inner 	Supported, optionally specify watermark on both sides + time constraints for state cleanup
     *                    Left Outer 	Conditionally supported, must specify watermark on right + time constraints for correct results, optionally specify watermark on left for all state cleanup
     *                    Right Outer 	Conditionally supported, must specify watermark on left + time constraints for correct results, optionally specify watermark on right for all state cleanup
     *                    Full Outer 	Conditionally supported, must specify watermark on one side + time constraints for correct results, optionally specify watermark on the other side for all state cleanup
     *                    Left Semi 	Conditionally supported, must specify watermark on right + time constraints for correct results, optionally specify watermark on left for all state cleanup
     * */
  }

  def main(args: Array[String]): Unit = {
    // joinStreamWithStatic()
    joinStreamWithStream()
  }
}
