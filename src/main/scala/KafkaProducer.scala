import org.json4s._
import org.json4s.jackson.Serialization.write

import scala.io.{BufferedSource, Source}
import scala.util.Try

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

//./kafka-topics --zookeeper localhost:2181 --create --topic books --partitions 3 --replication-factor 1

object KafkaProducer extends App {

  case class Book(
                 name: String,
                 author: String,
                 userRating: Double,
                 reviews: Int,
                 price: Int,
                 year: Int,
                 genre: String
                 )

  def parseHeaders(header: String): Map[String, Int] =
    header.split(",").zipWithIndex.toMap

  def parseData(data: String, headers: Map[String, Int]): Try[Book] = {
    val tokens: Array[String] = data.split(",")

    Try {
      Book(
        tokens(headers("Name")),
        tokens(headers("Author")),
        tokens(headers("User Rating")).toDouble,
        tokens(headers("Reviews")).toInt,
        tokens(headers("Price")).toInt,
        tokens(headers("Year")).toInt,
        tokens(headers("Genre"))
      )
    }
  }

  val fileName: String = "bestsellers with categories.csv"
  val csvFile: Try[BufferedSource] = Try { Source.fromResource(fileName) }

  if (csvFile.isSuccess) {
    val header :: data = csvFile.get.getLines.toList
    val headers: Map[String, Int] = parseHeaders(header)
    val books: List[Try[Book]] = data.map(parseData(_, headers))

    val successTransform: List[Book] = books.filter(_.isSuccess).map(_.get)
//    println(books.count(_.isSuccess))

    implicit val formats: DefaultFormats.type = DefaultFormats
    val jsonList: List[(String, String)] = successTransform
      .map(book => write(book))
      .zipWithIndex
      .map(el => (el._2.toString, el._1))

    val failedTransform = books.filter(_.isFailure)
//    println(books.filter(_.isFailure))
//    println(books.count(_.isFailure))

    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:29092")

    Try {
      val producer: KafkaProducer[String, String] = new KafkaProducer(props, new StringSerializer, new StringSerializer)
      jsonList.foreach(json => producer.send(new ProducerRecord("books", json._1 , json._2)))
      producer.close()
    }
  }

}
