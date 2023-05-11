package spark

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{SaveMode, SparkSession}



object HdfsToCassandra {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("HdfsToCassandra")
      .master("local[*]")
      .config("spark.cassandra.connection.host", "cassandra")
      .config("spark.cassandra.connection.port", "9042")
      .getOrCreate()

    // Read data from HDFS into a DataFrame
    val hdfsData = spark.read.format("csv")
      .option("header", "true")
      .load("hdfs://hadoop-master:9000/user/root/sp500/sp500/AAPL.csv")

    hdfsData.show()


   /* // user derfined function that transforms string date to Cassandra Date
    val stringToDate: String => LocalDate = (dateString: String) => {
      val date = java.time.LocalDate.parse(dateString, DateTimeFormatter.ofPattern("dd-MM-yyyy"))
      com.datastax.driver.core.LocalDate.fromMillisSinceEpoch(date.atStartOfDay(ZoneId.systemDefault()).toInstant.toEpochMilli)
    }
    val stringToDateUDF = udf(stringToDate)*/


    // Write the DataFrame to Cassandra
    hdfsData
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "historical_data_aapl", "keyspace" -> "finance"))
      .mode(SaveMode.Append)
      .save()
    // Stop the SparkSession
    spark.stop()
  }
}
