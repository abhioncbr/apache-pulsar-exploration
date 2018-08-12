package com.tweets.pulsar.producer

import com.tweets.util.Util
import org.apache.pulsar.client.api.{Producer, PulsarClient}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{StreamingContext, _}
import org.apache.spark.streaming.twitter._

@deprecated
//creating producer for each rdd partition (requirement is to have one pulsar producer per executor)
class PuslarClientWrapper(pulsarUrl: String, topic: String) extends Serializable {

  @transient lazy private val client: PulsarClient = PulsarClient.builder.serviceUrl(pulsarUrl).build
  @transient lazy private val producer: Producer[Array[Byte]] = client.newProducer.topic(topic).create

  def send(str: String): Unit ={
    producer.send(str.getBytes)
  }

  def close(): Unit ={
    producer.flush()
    producer.close()
    client.close()
  }
}

// added wrapper class for pulsar producer, so that per producer per executor will publish tweets.
class PuslarSink(createProducer: () => Producer[Array[Byte]] ) extends Serializable {
  @transient lazy private val producer = createProducer()
  def send(value: String): Unit = producer.send(value.getBytes)
}

object PuslarSink {
  def apply(config: Map[String, String]): PuslarSink = {
    val f = () => {
      val client: PulsarClient = PulsarClient.builder.serviceUrl(config("pulsarUrl")).build
      val producer: Producer[Array[Byte]] = client.newProducer.topic(config("topic")).create

      sys.addShutdownHook {
        producer.flush()
        producer.close()
        client.close()
      }

      producer
    }
    new PuslarSink(f)
  }
}

class TweetsPulsarProducer(args: Array[String]) {
  def run(): Unit = {
    //setting up spark-streaming context
    val duration: Duration = Seconds(20)
    val sparkSession: SparkSession = SparkSession.builder().appName("tweetsPublisher").getOrCreate()
    val streamingContext = new StreamingContext(sparkSession.sparkContext, duration)

    //setting up pulsar client & topic producer
    //val producer = new PuslarClientWrapper( args(0), args(1))
    val conf: Map[String, String] = Map("pulsarUrl" -> args(0), "topic" -> args(1))
    val pulsarSink: Broadcast[PuslarSink] = sparkSession.sparkContext.broadcast(PuslarSink(conf))
    publishTweets(streamingContext, pulsarSink, args(2))

    // starting tweets publishing to pulsar
    streamingContext.start
    streamingContext.awaitTermination
    //producer.close()
  }

  def publishTweets(streamingContext: StreamingContext, pulsarSink: Broadcast[PuslarSink], tweetsFilter: String): Unit  = {
    val stream = TwitterUtils.createStream(streamingContext, None)
    val filteredTweets = stream.filter(status => status.getText.split(" ")
      .toSet.exists(str => str.contains(tweetsFilter))).map(status => status.getText.replace('\n', ' '))

    filteredTweets.foreachRDD {
      rdd => rdd.foreachPartition {
        iterator => iterator.foreach {
          tweet => pulsarSink.value.send(tweet)
        }
      }
    }
  }
}

object TweetsPulsarProducer extends App {
  println(args.mkString(" : "))
  if (args.length < 4) {
    System.err.println("Usage: TweetsPulsarProducer <pulsarUrl> <pulsar-topic> <tweetsFilter> <twitterPropertiesPath>")
    System.exit(1)
  }
  val twitterProperties: Map[String,String] = Util.readProperties(args(3))
  //setting up twitter keys.
  System.setProperty("twitter4j.oauth.consumerKey", twitterProperties.getOrElse("consumerKey", null) )
  System.setProperty("twitter4j.oauth.consumerSecret", twitterProperties.getOrElse("consumerSecret", null) )
  System.setProperty("twitter4j.oauth.accessToken", twitterProperties.getOrElse("accessToken", null) )
  System.setProperty("twitter4j.oauth.accessTokenSecret", twitterProperties.getOrElse("accessTokenSecret", null) )

  val publisher = new TweetsPulsarProducer(args)
  publisher.run()
}

