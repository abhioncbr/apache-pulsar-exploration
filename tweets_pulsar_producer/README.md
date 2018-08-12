# tweets_pulsar_producer [repo for publishing tweets to pulsar-cluster]
* Consist various classes for exploring various pulsar producer options.
* For execution use apache-pulsar version 2.1.0-incubating & spark version 2.2.0. 
* project is based on scala & sbt as build tool.
* TweetsPulsarProducer is simple class using spark streaming for publishing tweets to pulsar cluster.
  * for getting tweets from twitter, [twitter4j](http://twitter4j.org/en/) library is getting used.
  * update [twitter.properties](src/main/resources/twitter.properties) file with Twitter access token credentials. For getting credentials, register your [app](https://developer.twitter.com/en/apps/create) on Twitter.
  * for testing, run pulsar in [standalone mode](https://pulsar.incubator.apache.org/docs/en/standalone/)
  * run code using following command 
    ``./spark-submit --class com.tweets.pulsar.producer.TweetsPulsarProducer <tweets_pulsar_producer_jar_path> pulsar://localhost:6650 tweets stream <twitter.propreties path>``