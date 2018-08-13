package com.tweets.util

import com.tweets.pulsar.producer.TweetsPulsarProducer.twitterProperties

import scala.io.Source.fromFile

object Util {

  def readProperties(filePath: String): Map[String,String] = {
    val lines = fromFile(filePath).getLines.toSeq
    val cleanLines = lines.map(_.trim).filter(!_.startsWith("#")).filter(_.contains("="))
    cleanLines.map(line => { val Array(a,b) = line.split("=",2);(a.trim, b.trim)}).toMap
  }

  def setTwitterProperties(twitterProperties: Map[String, String]): Unit ={
    //setting up twitter keys.
    System.setProperty("twitter4j.oauth.consumerKey", twitterProperties.getOrElse("consumerKey", null) )
    System.setProperty("twitter4j.oauth.consumerSecret", twitterProperties.getOrElse("consumerSecret", null) )
    System.setProperty("twitter4j.oauth.accessToken", twitterProperties.getOrElse("accessToken", null) )
    System.setProperty("twitter4j.oauth.accessTokenSecret", twitterProperties.getOrElse("accessTokenSecret", null) )
  }
}
