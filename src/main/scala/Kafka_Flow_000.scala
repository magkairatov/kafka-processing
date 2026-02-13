package com.github.magkairatov

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.typesafe.config.Config
import net.liftweb.json.{DefaultFormats, Formats, ShortTypeHints}
//import org.json4s.{DefaultFormats, Formats}

abstract class Kafka_Flow_000(implicit mat: Materializer, system: ActorSystem, config: Config) extends Kafka_Flow {
  //implicit val formats: Formats = DefaultFormats
  implicit val ec = mat.executionContext

  protected val flowId: String = this.getClass.getSimpleName.toLowerCase

  implicit val des = Kafka_Streams_Core.KafkaConsumerAvroDeSettings(flowId)
  implicit val jsonConsumerSettings = Kafka_Streams_Core.KafkaConsumerJsonSettings(flowId)
  val producer = Kafka_Streams_Core.Producer

  implicit val formats: Formats = new DefaultFormats {
    outer =>
    override val typeHintFieldName = "type"
    override val typeHints = ShortTypeHints(List(classOf[String], classOf[Double], classOf[Double], classOf[Double], classOf[Double], classOf[Double], classOf[Double], classOf[Double], classOf[Double], classOf[Double], classOf[Double], classOf[Double], classOf[Double], classOf[Double], classOf[Double], classOf[Double], classOf[Double], classOf[String], classOf[String]))
  }
}
