package com.tweets.pulsar.consumer

import java.io.{BufferedWriter, File, FileWriter}
import com.tweets.PulsarConsumerSink


class TweetsPulsarConsumer(args: Array[String]) {
  def run(): Unit = {
    val conf: Map[String, String] = Map("pulsarUrl" -> args(0), "topic" -> args(1))
    val pulsarConsumer: PulsarConsumerSink[Array[Byte]] = PulsarConsumerSink(conf, classOf[Array[Byte]])

    consumeTweets(pulsarConsumer, args(2))
  }


  def consumeTweets(consumer: PulsarConsumerSink[Array[Byte]], filePath: String ): Unit ={
    val file = new File(filePath)
    val bw = new BufferedWriter(new FileWriter(file))

   println(s"consumed messages are getting published to $filePath")
    do {
      val message: String = new String(consumer.receive)
      bw.write(message
      )
      bw.newLine()
    } while(!Thread.interrupted())

    bw.flush()
    bw.close()
    println(s"file closed: $filePath")
  }

}

object TweetsPulsarConsumer extends App{
  if (args.length < 2) {
    println("TweetsPulsarConsumer <pulsarurl> <topic>")
  }
  val consumer = new TweetsPulsarConsumer(args)
  consumer.run()
}
