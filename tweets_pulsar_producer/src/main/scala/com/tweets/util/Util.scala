package com.tweets.util

import scala.io.Source.fromFile

object Util {

  def readProperties(filePath: String): Map[String,String] = {
    val lines = fromFile(filePath).getLines.toSeq
    val cleanLines = lines.map(_.trim).filter(!_.startsWith("#")).filter(_.contains("="))
    cleanLines.map(line => { val Array(a,b) = line.split("=",2);(a.trim, b.trim)}).toMap
  }
}
