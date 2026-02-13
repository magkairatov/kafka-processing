package com.github.magkairatov

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink}
import akka.stream.{ActorAttributes, ClosedShape, Materializer, RestartSettings}
import com.sksamuel.avro4s.RecordFormat
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import akka.stream.scaladsl.RestartSource
import akka.http.scaladsl.model.{ContentTypes, HttpMethods}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import java.text.SimpleDateFormat


class Kafka_Flow_001(implicit mat: Materializer,
                     log: Logger,
                     system: ActorSystem,
                     config: Config
                    ) extends Kafka_Flow_000 {

  case class Transaction(
                          ROWKEY: String,
                          CPID: Option[String],
                          CPRN: Option[String],
                          CPNA: Option[String],
                          ACQUIRE: Option[String],
                          TERMINAL: Option[String],
                          STAMP: Option[String]
                        )

  case class Record(
                          CPID: Option[String],
                          CPRN: Option[String],
                          CPNA: Option[String],
                          ACQUIRE: Option[String],
                          TERMINAL: Option[String],
                          STAMP: Option[Long]
                        )

  private val avroSchemaOutput = RecordFormat[Transaction]
  private val avroSchemaOutput2 = RecordFormat[Record]

  val backoffSettings = RestartSettings(
    minBackoff = 1.minute,
    maxBackoff = 15.minutes,
    randomFactor = 0.2
  ).withMaxRestarts(20, 1.hour)

  private def source(implicit kafkaConsumerJsonSettings: ConsumerSettings[String, String]) =
    RestartSource.withBackoff(backoffSettings) { () =>
      Consumer.committableSource(kafkaConsumerJsonSettings, Subscriptions.topics("jms_from_way4"))
        .map { p =>
          val key = Option(p.record.key()).getOrElse("null")
          val value = Option(p.record.value()).getOrElse("")
          (key, value)
        }
        .filter { case (key, value) =>
          // Filter for messages with t: "doc.trn"
          isDocTrnMessage(value)
        }
        .map { case (key, value) =>
          //println(s"Processing doc.trn message with key: $key, $value")
          parseJsonMessage(value)
        }
        .collect {
          case Some(transaction) =>
            //println(s"Successfully collected transaction: ${transaction.ROWKEY}")
            transaction
        }
    }

  private def isDocTrnMessage(jsonString: String): Boolean = {
    Try {
      import net.liftweb.json._
      implicit val formats: Formats = DefaultFormats
      val json = parse(jsonString)
      (json \ "t").extractOpt[String].contains("doc.trn")
    }.getOrElse(false)
  }

  private def parseJsonMessage(jsonString: String): Option[Transaction] = {
    Try {
      import net.liftweb.json._
      implicit val formats: Formats = DefaultFormats

      if (jsonString == null || jsonString.trim.isEmpty) {
        throw new IllegalArgumentException("JSON string is null or empty")
      }

      val json = try {
        parse(jsonString)
      } catch {
        case ex: Exception =>
          throw new RuntimeException(s"JSON parsing failed: ${ex.getClass.getSimpleName}: ${ex.getMessage}", ex)
      }

      val vObject = json \ "v"

      // Helper function to safely extract String fields, treating "null" strings as None
      def safeExtractString(field: JValue): Option[String] = {
        field match {
          case JNull => None
          case JString(s) if s == "null" || s.trim.isEmpty => None
          case JString(s) => Some(s)
          case JInt(num) => Some(num.toString)
          case JDouble(num) => Some(num.toString)
          case JBool(b) => Some(b.toString)
          case _ =>
            try {
              field.extractOpt[String] match {
                case Some(value) if value == "null" || value.trim.isEmpty => None
                case Some(value) => Some(value)
                case None => None
              }
            } catch {
              case ex: Exception =>
                //println(s"Unexpected error extracting string field: ${ex.getMessage}")
                None
            }
        }
      }

      val id = try {
        (vObject \ "id").extractOpt[String].getOrElse("0")
      } catch {
        case ex: Exception =>
          //println(s"Error extracting ID field: ${ex.getMessage}")
          "0"
      }
      val stamp = safeExtractString(vObject \ "stamp")

      val sMemberId = safeExtractString(vObject \ "s_member_id")
      val sNumber = safeExtractString(vObject \ "s_number")

      val infoString = safeExtractString(vObject \ "info")
      val cpid = extractFromInfo(infoString, "CPID")
      val cprn = extractFromInfo(infoString, "CPRN")
      val cpna = extractFromInfo(infoString, "CPNA")

      Transaction(
        id,
        cpid,
        cprn,
        cpna,
        sMemberId,
        sNumber,
        stamp
      )
    } match {
      case Success(transaction) =>
        //println(s"Successfully parsed transaction: $transaction")
        Some(transaction)
      case Failure(ex) =>
        log.error(s"Failed to parse JSON message: ${ex.getClass.getSimpleName}: ${Option(ex.getMessage).getOrElse("No error message")}")
        if (ex.getCause != null) {
          //println(s"Caused by: ${ex.getCause.getClass.getSimpleName}: ${Option(ex.getCause.getMessage).getOrElse("No cause message")}")
        }
        //println(s"Failed JSON (first 500 chars): ${jsonString.take(500)}")
        None
    }
  }

  private def extractFromInfo(infoString: Option[String], fieldName: String): Option[String] = {
    infoString.flatMap { info =>
      val pattern = s"""$fieldName=([^;]+)""".r
      pattern.findFirstMatchIn(info) match {
        case Some(m) => Some(m.group(1))
        case None => None
      }
    }
  }

  private def processingFlow = {
    Flow[Transaction]
      .map { transaction =>
        Try {
          //println(s"Processing transaction: $transaction")

          val key = transaction.ROWKEY

          val avroRecord = Try {
            avroSchemaOutput.to(transaction)
          } match {
            case Success(record) =>
              //println(s"Successfully converted transaction ${transaction.ROWKEY} to AVRO")
              record
            case Failure(ex) =>
              log.error(s"Failed to convert transaction to AVRO: ${ex.getMessage}", ex)
              throw ex
          }

          val stampMillis: Option[String] = transaction.STAMP.map { stampStr =>
            Try {
              val targetTimeZone = "UTC"

              val tzFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
              tzFormat.setTimeZone(java.util.TimeZone.getTimeZone(targetTimeZone))

              val tzFormat1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
              tzFormat1.setTimeZone(java.util.TimeZone.getTimeZone(targetTimeZone))

              val tzFormat2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm")
              tzFormat2.setTimeZone(java.util.TimeZone.getTimeZone(targetTimeZone))

              val parsedDate = Try {
                tzFormat.parse(stampStr) // "yyyy-MM-dd'T'HH:mm:ss.SSS"
              }.orElse(Try {
                tzFormat1.parse(stampStr) // "yyyy-MM-dd'T'HH:mm:ss"
              }).orElse(Try {
                tzFormat2.parse(stampStr) // "yyyy-MM-dd'T'HH:mm"
              }).get

              parsedDate.getTime.toString
            } match {
              case Success(millis) =>
                //println(s"Successfully converted STAMP '$stampStr' to milliseconds: $millis")
                millis
              case Failure(ex) =>
                log.warn(s"Failed to convert STAMP '$stampStr': ${ex.getMessage}")
                System.currentTimeMillis().toString
            }
          }

          val producerRecord = new ProducerRecord[String, AnyRef]("bank_statement", key, avroSchemaOutput2.to(
            Record(transaction.CPID,
              transaction.CPRN,
              transaction.CPNA,
              transaction.ACQUIRE,
              transaction.TERMINAL,
              stampMillis.map(_.toLong))))

          //println(s"Created producer record for transaction ${transaction.ROWKEY}")
          producerRecord
        } match {
          case Success(record) =>
            //println(s"Successfully processed transaction ${transaction.ROWKEY}")
            record
          case Failure(ex) =>
            log.error(s"Error processing transaction ${transaction.ROWKEY}: ${ex.getMessage}", ex)
            throw ex
        }
      }
  }

  private val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
    val bcast = builder.add(Broadcast[Transaction](1))

    source ~> bcast.in
    bcast.out(0) ~> processingFlow ~> producer


    ClosedShape
  })

  override def run() = {
    graph.withAttributes(ActorAttributes.supervisionStrategy(globalSupervisorException)).run()
  }

  //private val cs = Charset.forName("UTF-8")

}