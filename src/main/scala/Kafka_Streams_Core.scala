package com.github.magkairatov

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Producer
import akka.kafka.{ConsumerSettings, ProducerSettings}
import io.confluent.kafka.serializers.{AbstractKafkaSchemaSerDeConfig, KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import com.typesafe.config.Config
import com.sksamuel.avro4s.RecordFormat
import scala.jdk.CollectionConverters._

class Kafka_Streams_Core(implicit config: Config, system: ActorSystem) {
  private lazy val (kafkaAvroSerializer,kafkaAvroDeserializer) = {
    val kafkaAvroSerDeConfig = Map[String, Any](
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> config.getString("schema-registry.url"),
      AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION -> true.toString,
    )
    val avroSer = new KafkaAvroSerializer
    avroSer.configure(kafkaAvroSerDeConfig.asJava, false)
    val avroDe = new KafkaAvroDeserializer
    avroDe.configure(kafkaAvroSerDeConfig.asJava, false)
    (avroSer,avroDe)
  }

  def kafkaConsumerJsonSettings(flowId: String = "default") = ConsumerSettings(
    system,
    new StringDeserializer,
    new StringDeserializer  
  ).withGroupId(s"${config.getString("kafka.groupid")}-$flowId")

  private val kafkaSettingsAvroSerWithProducer = {
    val producerSettings = ProducerSettings(config.getConfig("akka.kafka.producer"), new StringSerializer, kafkaAvroSerializer)
    val kafkaProducer = producerSettings.createKafkaProducer()
    producerSettings.withProducer(kafkaProducer)
  }

  def kafkaConsumerAvroDeSettings(flowId: String = "default") = ConsumerSettings(system, new StringDeserializer, kafkaAvroDeserializer)
    .withGroupId(s"${config.getString("kafka.groupid")}-avro-$flowId")

  private lazy val producer = Producer.plainSink(Kafka_Streams_Core.KafkaSettingsAvroSerWithProducer)

  private lazy val flexiProducer = Producer.flexiFlow(kafkaSettingsAvroSerWithProducer.asInstanceOf[ProducerSettings[String,com.sksamuel.avro4s.Record]])
}

object Kafka_Streams_Core {
  private var instance: Kafka_Streams_Core = null

  def KafkaSettingsAvroSerWithProducer(implicit config: Config, system: ActorSystem) = {
    if (instance == null) {
      instance = new Kafka_Streams_Core()
    }
    instance.kafkaSettingsAvroSerWithProducer
  }

  def KafkaConsumerAvroDeSettings(flowId: String = "default")(implicit config: Config, system: ActorSystem) = {
    if (instance == null) {
      instance = new Kafka_Streams_Core()
    }
    instance.kafkaConsumerAvroDeSettings(flowId)
  }

  def KafkaConsumerJsonSettings(flowId: String = "default")(implicit config: Config, system: ActorSystem) = {
    if (instance == null) {
      instance = new Kafka_Streams_Core()
    }
    instance.kafkaConsumerJsonSettings(flowId)
  }

  def Producer(implicit config: Config, system: ActorSystem) = {
    if (instance == null) {
      instance = new Kafka_Streams_Core()
    }
    instance.producer
  }

  def FlowProducer(implicit config: Config, system: ActorSystem) = {
    if (instance == null) {
      instance = new Kafka_Streams_Core()
    }
    instance.flexiProducer
  }
}