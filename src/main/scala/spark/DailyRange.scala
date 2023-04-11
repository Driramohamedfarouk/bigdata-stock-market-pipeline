package spark

import com.datastax.spark.connector.streaming.toDStreamFunctions
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object DailyRange {
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
    //val formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy")

    val dailyRange = lines.map(line=>{
      val fields = line.split(",")
      val ticker = fields(0).split("/")(2).split("\\.")(0)
      val high = fields(5).toDouble
      val low = fields(2).toDouble
      val diff = high-low
      //val date = LocalDate.parse(fields(1), formatter)
      //println(date)
      //val cassandraDate = com.datastax.driver.core.LocalDate.fromDaysSinceEpoch(date.toEpochDay.toInt)
      //println(cassandraDate)
      (ticker,fields(1),diff)
    }).saveToCassandra("finance","daily_range")
    ssc.start()
    ssc.awaitTermination()
  }
}
