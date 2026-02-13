package com.github.magkairatov

import akka.NotUsed
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
import net.liftweb.json.{DefaultFormats, Formats, JNull, JValue}

import java.text.SimpleDateFormat
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}


class Kafka_Flow_002(implicit mat: Materializer,
                     log: Logger,
                     system: ActorSystem,
                     config: Config
                    ) extends Kafka_Flow_000 {

  case class CapTrnTransaction(
                                ID: Option[Long],
                                PID: Option[Long],
                                PRID: Option[Long],
                                OP: Option[String],
                                CNLID: Option[String],
                                CNLNM: Option[String],
                                TT: Option[String],
                                TN: Option[String],
                                IBAN: Option[String],
                                TAX: Option[String],
                                AM: Option[Double],
                                CUR: Option[String],
                                RATE: Option[Double],
                                RF: Option[String],
                                DSC: Option[String],
                                BAL: Option[Double],
                                BALCH: Option[Double],
                                REV: Option[String],
                                KNP: Option[String],
                                RRN: Option[String],
                                LOC: Option[String],
                                PRP: Option[String],
                                STAMP: Option[Long],
                                KEY: Option[Int],
                                CC: Option[String],
                                PLAN: Option[String],
                                FIN: Option[Int],
                                IM: Option[Int],
                                HLDFL: Option[Int],
                                BKMKR: Option[Int],
                                DT: Option[String]
                              )

  private val avroSchemaCapTrn = RecordFormat[CapTrnTransaction]

  val backoffSettings = RestartSettings(
    minBackoff = 1.minute,
    maxBackoff = 15.minutes,
    randomFactor = 0.2
  ).withMaxRestarts(20, 1.hour)

  private def source(implicit kafkaConsumerJsonSettings: ConsumerSettings[String, String]) =
    RestartSource.withBackoff(backoffSettings) { () =>
      Consumer.committableSource(kafkaConsumerJsonSettings, Subscriptions.topics("jms_from_dwh"))
        .map { p =>
          val key = Option(p.record.key()).getOrElse("null")
          val value = Option(p.record.value()).getOrElse("")
          (key, value)
        }
        .filter { case (key, value) =>
          isCapTrnMessage(value)
        }
        .map { case (key, value) =>
          //println(s"Processing captrn message with key: $key")
          //println(s"Processing captrn message content: $value")
          parseCapTrnMessage(value)
        }
        .collect {
          case Some(capTrnTransaction) =>
            //println(s"Successfully collected cap trn transaction: ${capTrnTransaction.ID}")
            capTrnTransaction
        }
    }

  private def isCapTrnMessage(jsonString: String): Boolean = {
    Try {
      import net.liftweb.json._
      implicit val formats: Formats = DefaultFormats
      val json = parse(jsonString)
      (json \ "t").extractOpt[String].contains("captrn")
    }.getOrElse(false)
  }

  private def parseCapTrnMessage(jsonString: String): Option[CapTrnTransaction] = {
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

      def safeExtractLong(field: JValue): Option[Long] = {
        field match {
          case JNull => None
          case JString(s) if s == "null" || s.trim.isEmpty => None
          case JString(s) => Some(s.toLong)
          case JInt(num) => Some(num.toLong)
          case JDouble(num) => Some(num.toLong)
          case _ =>
            try {
              field.extractOpt[Long]
            } catch {
              case _: Exception =>
                field.extractOpt[String].flatMap(s => Try(s.toLong).toOption)
            }
        }
      }

      def safeExtractInt(field: JValue): Option[Int] = {
        field match {
          case JNull => None
          case JString(s) if s == "null" || s.trim.isEmpty => None
          case JString(s) => Try(s.toInt).toOption
          case JInt(num) => Some(num.toInt)
          case JDouble(num) => Some(num.toInt)
          case _ =>
            try {
              field.extractOpt[Int]
            } catch {
              case _: Exception =>
                field.extractOpt[String].flatMap(s => Try(s.toInt).toOption)
            }
        }
      }

      def safeExtractDouble(field: JValue): Option[Double] = {
        field match {
          case JNull => None
          case JString(s) if s == "null" || s.trim.isEmpty => None
          case JString(s) => Try(s.toDouble).toOption
          case JInt(num) => Some(num.toDouble)
          case JDouble(num) => Some(num)
          case _ =>
            try {
              field.extractOpt[Double]
            } catch {
              case _: Exception =>
                field.extractOpt[String].flatMap(s => Try(s.toDouble).toOption)
            }
        }
      }

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
                log.error(s"Unexpected error extracting string field: ${ex.getMessage}")
                None
            }
        }
      }

      val stampLong = safeExtractString(vObject \ "stamp").flatMap { stampStr =>
        Try {
          val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          dateFormat.parse(stampStr).getTime
        }.toOption.orElse {
          Try {
            val dateFormat2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
            dateFormat2.parse(stampStr).getTime
          }.toOption
        }
      }

      CapTrnTransaction(
        ID = safeExtractLong(vObject \ "id"),
        PID = safeExtractLong(vObject \ "pid"),
        PRID = safeExtractLong(vObject \ "prid"),
        OP = safeExtractString(vObject \ "op"),
        CNLID = safeExtractString(vObject \ "cnlid"),
        CNLNM = safeExtractString(vObject \ "cnlnm"),
        TT = safeExtractString(vObject \ "tt"),
        TN = safeExtractString(vObject \ "tn"),
        IBAN = safeExtractString(vObject \ "iban"),
        TAX = safeExtractString(vObject \ "tax"),
        AM = safeExtractDouble(vObject \ "am"),
        CUR = safeExtractString(vObject \ "cur"),
        RATE = safeExtractDouble(vObject \ "rate"),
        RF = safeExtractString(vObject \ "rf"),
        DSC = safeExtractString(vObject \ "dsc"),
        BAL = safeExtractDouble(vObject \ "bal"),
        BALCH = safeExtractDouble(vObject \ "balch"),
        REV = safeExtractString(vObject \ "rev"),
        KNP = safeExtractString(vObject \ "knp"),
        RRN = safeExtractString(vObject \ "rrn"),
        LOC = safeExtractString(vObject \ "loc"),
        PRP = safeExtractString(vObject \ "prp"),
        STAMP = stampLong,
        KEY = safeExtractInt(vObject \ "key"),
        CC = safeExtractString(vObject \ "cc"),
        PLAN = safeExtractString(vObject \ "plan"),
        FIN = safeExtractInt(vObject \ "fin"),
        IM = safeExtractInt(vObject \ "im"),
        HLDFL = safeExtractInt(vObject \ "hldfl"),
        BKMKR = safeExtractInt(vObject \ "bkmkr"),
        DT = safeExtractString(vObject \ "dt")
      )
    } match {
      case Success(capTrnTransaction) =>
        //println(s"Successfully parsed cap trn transaction: $capTrnTransaction")
        Some(capTrnTransaction)
      case Failure(ex) =>
        log.error(s"Failed to parse cap trn JSON message: ${ex.getClass.getSimpleName}: ${Option(ex.getMessage).getOrElse("No error message")}")
        if (ex.getCause != null) {
          log.error(s"Caused by: ${ex.getCause.getClass.getSimpleName}: ${Option(ex.getCause.getMessage).getOrElse("No cause message")}")
        }
        log.error(s"Failed JSON (first 500 chars): ${jsonString.take(500)}")
        None
    }
  }

  private def processingFlow = {
    Flow[CapTrnTransaction]
      .map { capTrnTransaction =>
        Try {
          //println(s"Processing cap trn transaction: ${capTrnTransaction.ID}")

          val key = capTrnTransaction.ID.map(_.toString).getOrElse("null")

          val producerRecord = new ProducerRecord[String, AnyRef]("dwh_raw_cap_trn", key, avroSchemaCapTrn.to(capTrnTransaction))

          //println(s"Created producer record for cap trn transaction ${capTrnTransaction.ID}")
          producerRecord
        } match {
          case Success(record) =>
            //println(s"Successfully processed cap trn transaction ${capTrnTransaction.ID}")
            record
          case Failure(ex) =>
            log.error(s"Error processing cap trn transaction ${capTrnTransaction.ID}: ${ex.getMessage}", ex)
            throw ex
        }
      }
  }

  private val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
    val bcast = builder.add(Broadcast[CapTrnTransaction](1))

    source ~> bcast.in
    bcast.out(0) ~> processingFlow ~> producer

    ClosedShape
  })

  override def run() = {
    //println("Starting Kafka_Flow_002 - DWH CapTrn processing")
    graph.withAttributes(ActorAttributes.supervisionStrategy(globalSupervisorException)).run()
  }
}