package spark

import com.datastax.spark.connector.streaming.toDStreamFunctions
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FilterHighVolume {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: SparkKafkaDailyRange <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("SparkKafkaDailyRange").
      set("spark.cassandra.connection.host","172.18.0.5")

    // Create the streaming context with a batch size of 2 seconds
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val numThreads = args(3).toInt
    val topicMap = args(2).split(",").map((_, numThreads)).toMap

    val messages = KafkaUtils.createStream(ssc, args(0), args(1), topicMap)

    val lines = messages.map(_._2)

    val highVolumeLines = lines.filter(line => {
      val values = line.split(",")
      val volume = values(4).toDouble
      volume > 600000
    })
    highVolumeLines.print()
    highVolumeLines.map(line=>{
      var arr = line.split(",");
      (arr(0),arr(1))
    }).saveToCassandra("finance","high_volume")

    ssc.start()
    ssc.awaitTermination()
  }
}
