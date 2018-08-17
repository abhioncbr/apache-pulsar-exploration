package com.tweets.pulsar.producer

import com.tweets.PulsarProducerSink
import com.tweets.util.Util
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{StreamingContext, _}

case class TweetsWithSchema(Id: Long, text: String, userScreenName: String, tweetDate: java.util.Date) { }
case class TweetsInfoWithSchema(Id: Long, favCount: Int, retweetCount: Int,  tweetLang: String, country: String) {}

class TweetsWithSchemaPulsarProducer(args: Array[String]) {
  def run(): Unit = {
    //setting up spark-streaming context
    val duration: Duration = Seconds(20)
    val sparkSession: SparkSession = SparkSession.builder().appName("tweetsPublisher").getOrCreate()
    val streamingContext = new StreamingContext(sparkSession.sparkContext, duration)

    //setting up pulsar client & topic producer
    //val producer = new PuslarClientWrapper( args(0), args(1))
    val tweetTopicConf: Map[String, String] = Map("pulsarUrl" -> args(0), "topic" -> args(1))
    val pulsarTweetSink: Broadcast[PulsarProducerSink[TweetsWithSchema]] = sparkSession.sparkContext.broadcast(PulsarProducerSink( tweetTopicConf, classOf[TweetsWithSchema] ))

    val tweetInfoTopicConf: Map[String, String] = Map("pulsarUrl" -> args(0), "topic" -> args(2))
    val pulsarTweetInfoSink: Broadcast[PulsarProducerSink[TweetsInfoWithSchema]] = sparkSession.sparkContext.broadcast(PulsarProducerSink( tweetInfoTopicConf, classOf[TweetsInfoWithSchema] ))

    //publishing tweets & tweets Info in separate topics
    publishTweets(streamingContext, pulsarTweetSink, pulsarTweetInfoSink, args(2))

    // starting tweets publishing to pulsar
    streamingContext.start
    streamingContext.awaitTermination
    //producer.close()
  }

  def publishTweets(streamingContext: StreamingContext,
                    pulsarTweetSink: Broadcast[PulsarProducerSink[TweetsWithSchema]],
                    pulsarTweetInfoSink: Broadcast[PulsarProducerSink[TweetsInfoWithSchema]],
                    tweetsFilter: String): Unit  = {

    val stream = TwitterUtils.createStream(streamingContext, None)
    val filteredTweets = stream.filter(status => status.getText.split(" ")
      .toSet.exists(str => str.contains(tweetsFilter))).map(status => {

      //tweets information
      val id: Long = status.getId

      val text: String = status.getText.replace('\n', ' ')
      val userScreenName: String = status.getUser.getScreenName
      val tweetDate: java.util.Date = status.getCreatedAt

      //tweets other information
      val favCount: Int = status.getFavoriteCount
      val retweetCount: Int = status.getRetweetCount
      val tweetLang: String = status.getLang
      val country: String = status.getPlace.getCountry

      val tweet = TweetsWithSchema(id, text, userScreenName, tweetDate)
      val tweetInfo = TweetsInfoWithSchema(id, favCount, retweetCount, tweetLang, country)
      (tweet, tweetInfo)
    })

    filteredTweets.foreachRDD {
      rdd => rdd.foreachPartition {
        iterator => iterator.foreach {
          tweetData =>
            pulsarTweetSink.value.send(tweetData._1, tweetData._1.Id.toString)
            pulsarTweetInfoSink.value.send(tweetData._2, tweetData._2.Id.toString)
        }
      }
    }
  }
}

object TweetsWithSchemaPulsarProducer extends App {
  println(args.mkString(" : "))
  if (args.length < 4) {
    System.err.println("Usage: TweetsWithSchemaPulsarProducer <pulsarUrl> <pulsar-tweets-topic> <puslar-tweets-info-topic> <tweetsFilter> <twitterPropertiesPath>")
    System.exit(1)
  }

  //setting uo the twitter keys for getting tweets from Twitter.
  val twitterProperties: Map[String,String] = Util.readProperties(args(3))
  Util.setTwitterProperties(twitterProperties)

  val publisher = new TweetsPulsarProducer(args)
  publisher.run()
}

