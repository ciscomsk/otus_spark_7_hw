import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import scala.jdk.CollectionConverters._
import java.util.Properties
import java.time.Duration
import java.util
import scala.util.Try

object KafkaConsumer extends App {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:29092")
  props.put("group.id", "consumerOtus")

  val consumer = new KafkaConsumer(props, new StringDeserializer, new StringDeserializer)

  val receive: Try[Unit] = Try {
    consumer.subscribe(List("books").asJavaCollection)

    consumer.poll(Duration.ofSeconds(1))
    val partitions: util.Set[TopicPartition] = consumer.assignment
    consumer.seekToEnd(partitions)

    val msgCount: Int = 5
    val topicsWithStartPos: List[(TopicPartition, Long)] = partitions
      .asScala
      .toList
      .map(part => (part, consumer.position(part) - msgCount))

    topicsWithStartPos.foreach(el => consumer.seek(el._1, el._2))

    consumer
      .poll(Duration.ofSeconds(1))
      .asScala
      .foreach(msg => println(msg.value))

    consumer.close()
  }

  if (receive.isSuccess) println("Receiving successfully end.")
  else {
    println("Exception raised during receiving.")
    println(receive.get)
  }

}
