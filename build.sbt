name := "apache_pulsar_exploration"
Common.settings
lazy val tweets_pulsar_producer = project in file("tweets_pulsar_producer")
lazy val tweets_pulsar_consumer = project in file("tweets_pulsar_consumer")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}