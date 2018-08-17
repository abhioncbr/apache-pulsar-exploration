name := "tweets_pulsar_consumer"
Common.settings

libraryDependencies += "org.apache.pulsar" % "pulsar-client" % "2.1.0-incubating" withSources()

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

