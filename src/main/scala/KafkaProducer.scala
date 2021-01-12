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

  // Сделано допущение, что колонка, содержащая имя, всегда первая,
  // остальные могут менять свое положение
  def parseData(data: String, headers: Map[String, Int]): Try[Book] = {
      if (!data.startsWith("\"")) {
        val tokens: Array[String] = data.split(",")

        Try {
          Book(
            tokens(0),
            tokens(headers("Author")),
            tokens(headers("User Rating")).toDouble,
            tokens(headers("Reviews")).toInt,
            tokens(headers("Price")).toInt,
            tokens(headers("Year")).toInt,
            tokens(headers("Genre"))
          )
        }
      }
      else {
        val nameAndOtherData: Array[String] =  data.split("\",")
        val name: String = nameAndOtherData(0)
        val otherData: Array[String] = nameAndOtherData.slice(1, 2).mkString("").split(",")
        val nameColPos: Int = 1

        Try {
          Book(
            name,
            otherData(headers("Author") - nameColPos),
            otherData(headers("User Rating") - nameColPos).toDouble,
            otherData(headers("Reviews") - nameColPos).toInt,
            otherData(headers("Price") - nameColPos).toInt,
            otherData(headers("Year") - nameColPos).toInt,
            otherData(headers("Genre") - nameColPos)
          )
        }
      }

  }

  val fileName: String = "bestsellers with categories.csv"
  val csvFile: Try[BufferedSource] = Try { Source.fromResource(fileName) }

  if (csvFile.isSuccess) {
    val header :: data = csvFile.get.getLines.toList
    val headers: Map[String, Int] = parseHeaders(header)
    val books: List[Try[Book]] = data.map(parseData(_, headers))

    val successTransform: List[Book] = books.filter(_.isSuccess).map(_.get)

    implicit val formats: DefaultFormats.type = DefaultFormats
    val jsonList: List[(String, String)] = successTransform
      .map(book => write(book))
      .zipWithIndex
      .map(el => (el._2.toString, el._1))

    val failedTransform: List[Try[Book]] = books.filter(_.isFailure)
    if (failedTransform.nonEmpty) println(s"${books.count(_.isFailure)} errors while parsing")

    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:29092")

    val send: Try[Unit] = Try {
      val producer: KafkaProducer[String, String] = new KafkaProducer(props, new StringSerializer, new StringSerializer)
      jsonList.foreach(json => producer.send(new ProducerRecord("books", json._1 , json._2)))
      producer.close()
    }

    if (send.isSuccess) println("Sending successfully end.")
    else {
      println("Exception raised during sending.")
      println(send.get)
    }
  }

}
