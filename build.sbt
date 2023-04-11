name := "scala-spark"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1",
  "org.apache.spark" %% "spark-streaming" % "2.2.1",
  "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.2.1",
  "org.apache.kafka" % "kafka-clients" % "0.8.2.0",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.2"
)


ThisBuild / assemblyMergeStrategy := {
  case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}
