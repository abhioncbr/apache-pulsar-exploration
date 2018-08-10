name := "tweets_pulsar_producer"
Common.settings

val versions = new {
  val sparkVersion = "2.2.0"
  val twitterStreamingVersion = "2.2.1"
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % versions.sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % versions.sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % versions.sparkVersion % "provided",
  "org.apache.bahir" %% "spark-streaming-twitter" % versions.twitterStreamingVersion
)

