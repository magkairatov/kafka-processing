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


class Kafka_Flow_003(implicit mat: Materializer,
                     log: Logger,
                     system: ActorSystem,
                     config: Config
                    ) extends Kafka_Flow_000 {

  case class CbsTrnTransaction(
                                ID: Option[Long],
                                TRNID: Option[Long],
                                NORD1: Option[Int],
                                TAX1: Option[String],
                                CCODE1: Option[String],
                                JFL1: Option[Boolean],
                                ACC1: Option[String],
                                NAT1: Option[Double],
                                AM1: Option[Double],
                                CID1: Option[Int],
                                CUR1: Option[String],
                                INOUT1: Option[Int],
                                A1: Option[Int],
                                OP1: Option[String],
                                NM1: Option[String],
                                CLIDEPID1: Option[Int],
                                CLIID1: Option[Int],
                                SP1: Option[String],
                                PLAN1: Option[String],
                                NORD2: Option[Int],
                                TAX2: Option[String],
                                CCODE2: Option[String],
                                JFL2: Option[Boolean],
                                ACC2: Option[String],
                                NAT2: Option[Double],
                                AM2: Option[Double],
                                CID2: Option[Int],
                                CUR2: Option[String],
                                INOUT2: Option[Int],
                                A2: Option[Int],
                                OP2: Option[String],
                                NM2: Option[String],
                                CLIDEPID2: Option[Int],
                                CLIID2: Option[Int],
                                SP2: Option[String],
                                PLAN2: Option[String],
                                ORDDEPID: Option[Int],
                                ORDID: Option[Int],
                                RF: Option[String],
                                VO: Option[String],
                                SRC: Option[String],
                                ORIGIN: Option[String],
                                NO: Option[String],
                                AGRCDT: Option[String],
                                AGRIMPDT: Option[String],
                                AGRRECVDT: Option[String],
                                JRNID: Option[Int],
                                PJRNID: Option[Int],
                                PJNORD: Option[Int],
                                KBK: Option[String],
                                KNP: Option[String],
                                KOD: Option[String],
                                KBE: Option[String],
                                DVAL: Option[String],
                                STAMP: Option[Long],
                                ORF: Option[String],
                                OBIC: Option[String],
                                OACC: Option[String],
                                ONM: Option[String],
                                TS: Option[Long],
                                ACC1DEPID: Option[Int],
                                ACC1ID: Option[Int],
                                ACC2DEPID: Option[Int],
                                ACC2ID: Option[Int],
                                OBNM: Option[String],
                                CAP1: Option[Long],
                                ACC1CAPFL: Option[Int],
                                ACC2CAPFL: Option[Int],
                                PRP: Option[String],
                                AGRDORD: Option[String],
                                RATE: Option[Double],
                                ORDCODE: Option[String],
                                KSOCODE: Option[String],
                                OTAX: Option[String],
                                PCOUNTRY: Option[String],
                                BCOUNTRY: Option[String],
                                FBNM: Option[String],
                                FBTAX: Option[String],
                                FKBE: Option[String],
                                FPNM: Option[String],
                                FPTAX: Option[String]
                              )

  private val avroSchemaCbsTrn = RecordFormat[CbsTrnTransaction]

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
          isCbsTrnMessage(value)
        }
        .map { case (key, value) =>
          //println(s"Processing cbstrn message with key: $key")
          //println(s"Processing cbstrn message content: $value")
          parseCbsTrnMessage(value)
        }
        .collect {
          case Some(cbsTrnTransaction) =>
            //println(s"Successfully collected cbs trn transaction: ${cbsTrnTransaction.ID}")
            cbsTrnTransaction
        }
    }

  private def isCbsTrnMessage(jsonString: String): Boolean = {
    Try {
      import net.liftweb.json._
      implicit val formats: Formats = DefaultFormats
      val json = parse(jsonString)
      (json \ "t").extractOpt[String].contains("cbstrn")
    }.getOrElse(false)
  }

  private def parseCbsTrnMessage(jsonString: String): Option[CbsTrnTransaction] = {
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
          case JString(s) => Some(s.toInt)
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
          case JString(s) => Some(s.toDouble)
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

      def safeExtractBoolean(field: JValue): Option[Boolean] = {
        field match {
          case JNull => None
          case JString(s) if s == "null" || s.trim.isEmpty => None
          case JString(s) => Try(s.toBoolean).toOption
          case JBool(b) => Some(b)
          case JInt(num) => Some(num != 0)
          case JDouble(num) => Some(num != 0.0)
          case _ =>
            try {
              field.extractOpt[Boolean]
            } catch {
              case _: Exception =>
                field.extractOpt[Int].map(_ == 1).orElse {
                  field.extractOpt[String].flatMap(s => Try(s.toBoolean).toOption)
                }
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

      def convertTimestampToLong(timestampStr: String): Option[Long] = {
        Try {
          val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
          dateFormat.parse(timestampStr).getTime
        }.toOption.orElse {
          Try {
            val dateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            dateFormat2.parse(timestampStr).getTime
          }.toOption
        }.orElse {
          Try {
            val dateFormat3 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
            dateFormat3.parse(timestampStr).getTime
          }.toOption
        }
      }

      val stampLong = safeExtractString(vObject \ "stamp").flatMap(convertTimestampToLong)
      val tsLong = safeExtractString(vObject \ "ts").flatMap(convertTimestampToLong)

      CbsTrnTransaction(
        ID = safeExtractLong(vObject \ "id"),
        TRNID = safeExtractLong(vObject \ "trnid"),
        NORD1 = safeExtractInt(vObject \ "nord1"),
        TAX1 = safeExtractString(vObject \ "tax1"),
        CCODE1 = safeExtractString(vObject \ "ccode1"),
        JFL1 = safeExtractBoolean(vObject \ "jfl1"),
        ACC1 = safeExtractString(vObject \ "acc1"),
        NAT1 = safeExtractDouble(vObject \ "nat1"),
        AM1 = safeExtractDouble(vObject \ "am1"),
        CID1 = safeExtractInt(vObject \ "cid1"),
        CUR1 = safeExtractString(vObject \ "cur1"),
        INOUT1 = safeExtractInt(vObject \ "inout1"),
        A1 = safeExtractInt(vObject \ "a1"),
        OP1 = safeExtractString(vObject \ "op1"),
        NM1 = safeExtractString(vObject \ "nm1"),
        CLIDEPID1 = safeExtractInt(vObject \ "clidepid1"),
        CLIID1 = safeExtractInt(vObject \ "cliid1"),
        SP1 = safeExtractString(vObject \ "sp1"),
        PLAN1 = safeExtractString(vObject \ "plan1"),
        NORD2 = safeExtractInt(vObject \ "nord2"),
        TAX2 = safeExtractString(vObject \ "tax2"),
        CCODE2 = safeExtractString(vObject \ "ccode2"),
        JFL2 = safeExtractBoolean(vObject \ "jfl2"),
        ACC2 = safeExtractString(vObject \ "acc2"),
        NAT2 = safeExtractDouble(vObject \ "nat2"),
        AM2 = safeExtractDouble(vObject \ "am2"),
        CID2 = safeExtractInt(vObject \ "cid2"),
        CUR2 = safeExtractString(vObject \ "cur2"),
        INOUT2 = safeExtractInt(vObject \ "inout2"),
        A2 = safeExtractInt(vObject \ "a2"),
        OP2 = safeExtractString(vObject \ "op2"),
        NM2 = safeExtractString(vObject \ "nm2"),
        CLIDEPID2 = safeExtractInt(vObject \ "clidepid2"),
        CLIID2 = safeExtractInt(vObject \ "cliid2"),
        SP2 = safeExtractString(vObject \ "sp2"),
        PLAN2 = safeExtractString(vObject \ "plan2"),
        ORDDEPID = safeExtractInt(vObject \ "orddepid"),
        ORDID = safeExtractInt(vObject \ "ordid"),
        RF = safeExtractString(vObject \ "rf"),
        VO = safeExtractString(vObject \ "vo"),
        SRC = safeExtractString(vObject \ "src"),
        ORIGIN = safeExtractString(vObject \ "origin"),
        NO = safeExtractString(vObject \ "no"),
        AGRCDT = safeExtractString(vObject \ "agrcdt"),
        AGRIMPDT = safeExtractString(vObject \ "agrimpdt"),
        AGRRECVDT = safeExtractString(vObject \ "agrrecvdt"),
        JRNID = safeExtractInt(vObject \ "jrnid"),
        PJRNID = safeExtractInt(vObject \ "pjrnid"),
        PJNORD = safeExtractInt(vObject \ "pjnord"),
        KBK = safeExtractString(vObject \ "kbk"),
        KNP = safeExtractString(vObject \ "knp"),
        KOD = safeExtractString(vObject \ "kod"),
        KBE = safeExtractString(vObject \ "kbe"),
        DVAL = safeExtractString(vObject \ "dval"),
        STAMP = stampLong,
        ORF = safeExtractString(vObject \ "orf"),
        OBIC = safeExtractString(vObject \ "obic"),
        OACC = safeExtractString(vObject \ "oacc"),
        ONM = safeExtractString(vObject \ "onm"),
        TS = tsLong,
        ACC1DEPID = safeExtractInt(vObject \ "acc1depid"),
        ACC1ID = safeExtractInt(vObject \ "acc1id"),
        ACC2DEPID = safeExtractInt(vObject \ "acc2depid"),
        ACC2ID = safeExtractInt(vObject \ "acc2id"),
        OBNM = safeExtractString(vObject \ "obnm"),
        CAP1 = safeExtractLong(vObject \ "cap1"),
        ACC1CAPFL = safeExtractInt(vObject \ "acc1capfl"),
        ACC2CAPFL = safeExtractInt(vObject \ "acc2capfl"),
        PRP = safeExtractString(vObject \ "prp"),
        AGRDORD = safeExtractString(vObject \ "agrdord"),
        RATE = safeExtractDouble(vObject \ "rate"),
        ORDCODE = safeExtractString(vObject \ "ordcode"),
        KSOCODE = safeExtractString(vObject \ "ksocode"),
        OTAX = safeExtractString(vObject \ "otax"),
        PCOUNTRY = safeExtractString(vObject \ "pcountry"),
        BCOUNTRY = safeExtractString(vObject \ "bcountry"),
        FBNM = safeExtractString(vObject \ "fbnm"),
        FBTAX = safeExtractString(vObject \ "fbtax"),
        FKBE = safeExtractString(vObject \ "fkbe"),
        FPNM = safeExtractString(vObject \ "fpnm"),
        FPTAX = safeExtractString(vObject \ "fptax")
      )
    } match {
      case Success(cbsTrnTransaction) =>
        //println(s"Successfully parsed cbs trn transaction: $cbsTrnTransaction")
        Some(cbsTrnTransaction)
      case Failure(ex) =>
        log.error(s"Failed to parse cbs trn JSON message: ${ex.getClass.getSimpleName}: ${Option(ex.getMessage).getOrElse("No error message")}")
        if (ex.getCause != null) {
          log.error(s"Caused by: ${ex.getCause.getClass.getSimpleName}: ${Option(ex.getCause.getMessage).getOrElse("No cause message")}")
        }
        log.error(s"Failed JSON (first 500 chars): ${jsonString.take(500)}")
        None
    }
  }

  private def processingFlow = {
    Flow[CbsTrnTransaction]
      .map { cbsTrnTransaction =>
        Try {
          //println(s"Processing cbs trn transaction: ${cbsTrnTransaction.ID}")

          val key = cbsTrnTransaction.ID.map(_.toString).getOrElse("null")

          val producerRecord = new ProducerRecord[String, AnyRef]("dwh_raw_cbs_trn", key, avroSchemaCbsTrn.to(cbsTrnTransaction))

          //println(s"Created producer record for cbs trn transaction ${cbsTrnTransaction.ID}")
          producerRecord
        } match {
          case Success(record) =>
            //println(s"Successfully processed cbs trn transaction ${cbsTrnTransaction.ID}")
            record
          case Failure(ex) =>
            log.error(s"Error processing cbs trn transaction ${cbsTrnTransaction.ID}: ${ex.getMessage}", ex)
            throw ex
        }
      }
  }

  private val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
    val bcast = builder.add(Broadcast[CbsTrnTransaction](1))

    source ~> bcast.in
    bcast.out(0) ~> processingFlow ~> producer

    ClosedShape
  })

  override def run() = {
    //println("Starting Kafka_Flow_003 - DWH CbsTrn processing")
    graph.withAttributes(ActorAttributes.supervisionStrategy(globalSupervisorException)).run()
  }
}