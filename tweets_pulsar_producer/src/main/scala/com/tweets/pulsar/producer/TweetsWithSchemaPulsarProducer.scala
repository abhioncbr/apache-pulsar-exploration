package com.tweets.pulsar.producer

import com.tweets.PulsarSink
import com.tweets.util.Util
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{StreamingContext, _}

case class TweetsWithSchema(text: String, userScreenName: String, lang: String, country: String, tweetDate: java.util.Date) { }

class TweetsWithSchemaPulsarProducer(args: Array[String]) {
  def run(): Unit = {
    //setting up spark-streaming context
    val duration: Duration = Seconds(20)
    val sparkSession: SparkSession = SparkSession.builder().appName("tweetsPublisher").getOrCreate()
    val streamingContext = new StreamingContext(sparkSession.sparkContext, duration)

    //setting up pulsar client & topic producer
    //val producer = new PuslarClientWrapper( args(0), args(1))
    val conf: Map[String, String] = Map("pulsarUrl" -> args(0), "topic" -> args(1))
    val pulsarSink: Broadcast[PulsarSink[TweetsWithSchema]] = sparkSession.sparkContext.broadcast(PulsarSink( conf, classOf[TweetsWithSchema] ))
    publishTweets(streamingContext, pulsarSink, args(2))

    // starting tweets publishing to pulsar
    streamingContext.start
    streamingContext.awaitTermination
    //producer.close()
  }

  def publishTweets(streamingContext: StreamingContext, pulsarSink: Broadcast[PulsarSink[TweetsWithSchema]], tweetsFilter: String): Unit  = {
    val stream = TwitterUtils.createStream(streamingContext, None)
    val filteredTweets = stream.filter(status => status.getText.split(" ")
      .toSet.exists(str => str.contains(tweetsFilter))).map(status => {
      val text = status.getText.replace('\n', ' ')
      val userScreenName = status.getUser.getScreenName
      val lang = status.getLang
      val country = status.getPlace.getCountry
      val tweetDate = status.getCreatedAt
      TweetsWithSchema(text, userScreenName, lang, country, tweetDate)
    })

    filteredTweets.foreachRDD {
      rdd => rdd.foreachPartition {
        iterator => iterator.foreach {
          tweetData => pulsarSink.value.send(tweetData, tweetData.userScreenName)
        }
      }
    }
  }
}

object TweetsWithSchemaPulsarProducer extends App {
  println(args.mkString(" : "))
  if (args.length < 4) {
    System.err.println("Usage: TweetsWithSchemaPulsarProducer <pulsarUrl> <pulsar-topic> <tweetsFilter> <twitterPropertiesPath>")
    System.exit(1)
  }

  //setting uo the twitter keys for getting tweets from Twitter.
  val twitterProperties: Map[String,String] = Util.readProperties(args(3))
  Util.setTwitterProperties(twitterProperties)

  val publisher = new TweetsPulsarProducer(args)
  publisher.run()
}

