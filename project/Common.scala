import sbt.Keys._
import sbt._

object Common {
  val settings: Seq[Setting[_]] = Seq(
    organization := "com.apache.pulsar.exploration",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.11.11"
  )
}
