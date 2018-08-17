package com.tweets

import org.apache.pulsar.client.api.{Consumer, PulsarClient, SubscriptionType, Message}
import org.apache.pulsar.client.impl.schema.JSONSchema

// added wrapper class for pulsar producer, so that per producer per executor will publish tweets.
class PulsarConsumerSink[T](createConsumer: () => Consumer[T] ) extends Serializable {
  @transient lazy private val consumer = createConsumer()

  def receive: T =  consumer.receive().getValue
  def receiveMessage: Message[T] = consumer.receive()
}

object PulsarConsumerSink {
  def apply[T](config: Map[String, Object], clazz: Class[T]): PulsarConsumerSink[T] = {
    val f = () => {

      val topic = config("topic").toString
      val subscription: SubscriptionType = config("subscription").toString.toLowerCase match {
        case "shared" => SubscriptionType.Shared
        case "failover" => SubscriptionType.Failover
        case "exclusive" => SubscriptionType.Exclusive
        case _ => throw new IllegalAccessException("No such pulsar subscription type.")
      }

      val client: PulsarClient = PulsarClient.builder.serviceUrl(config("pulsarUrl").toString).build
      val consumer: Consumer[T] = client.newConsumer(JSONSchema.of(clazz)).topic(topic).
                  subscriptionName(s"$topic-subscription").subscriptionType(subscription).subscribe

      sys.addShutdownHook {
        consumer.close()
        client.close()
      }

      consumer
    }
    new PulsarConsumerSink(f)
  }
}

