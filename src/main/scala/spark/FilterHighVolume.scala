package spark

import com.datastax.spark.connector.streaming.toDStreamFunctions
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.time.ZoneId
import java.time.format.DateTimeFormatter

object FilterHighVolume {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: SparkKafkaDailyRange <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("SparkKafkaDailyRange").
      set("spark.cassandra.connection.host","cassandra")

    // Create the streaming context with a batch size of 2 seconds
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val numThreads = args(3).toInt
    val topicMap = args(2).split(",").map((_, numThreads)).toMap

    val messages = KafkaUtils.createStream(ssc, args(0), args(1), topicMap)

    val lines = messages.map(_._2)

    val highVolumeLines = lines.filter(line => {
      val values = line.split(",")
      val volume = values(3).toDouble
      volume > 60000
    })
    highVolumeLines.print()
    highVolumeLines.map(line=>{
      val arr = line.split(",");
      val date = java.time.LocalDate.parse(arr(0), DateTimeFormatter.ofPattern("dd-MM-yyyy"))
      val eventDateCassandra = com.datastax.driver.core.LocalDate.fromMillisSinceEpoch(date.atStartOfDay(ZoneId.systemDefault()).toInstant.toEpochMilli)
      ("99051fe9-6a9c-46c2-b949-38ef78858dd0",eventDateCassandra,arr(3).toDouble)
    }).saveToCassandra("finance","high_volume_aapl")

    ssc.start()
    ssc.awaitTermination()
  }
}
