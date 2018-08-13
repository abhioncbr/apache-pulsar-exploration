package com.tweets

import org.apache.pulsar.client.api.{Producer, PulsarClient}
import org.apache.pulsar.client.impl.schema.JSONSchema
import scala.collection.JavaConverters._

// added wrapper class for pulsar producer, so that per producer per executor will publish tweets.
class PulsarSink[T](createProducer: () => Producer[T] ) extends Serializable {
  @transient lazy private val producer = createProducer()

  def send(value: T): Unit = producer.send(value)
  def send(value: T, key: String): Unit= producer.newMessage.key(key).value(value).send
  def send(value: T, key: String, prop: Map[String,String]): Unit= producer.newMessage.key(key).value(value).properties(prop.asJava).send
}

object PulsarSink {
  def apply[T](config: Map[String, Object], clazz: Class[T]): PulsarSink[T] = {
    val f = () => {
      val client: PulsarClient = PulsarClient.builder.serviceUrl(config("pulsarUrl").toString).build
      val producer: Producer[T] = client.newProducer(JSONSchema.of(clazz)).topic(config("topic").toString).create

      sys.addShutdownHook {
        producer.flush()
        producer.close()
        client.close()
      }

      producer
    }
    new PulsarSink(f)
  }
}
