package com.github.magkairatov

import java.net.InetAddress
import akka.actor.ActorSystem
import akka.stream.Materializer
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory
import akka.http.scaladsl.{ClientTransport, Http}

import java.net.InetSocketAddress
import scala.concurrent.Await
import scala.concurrent.duration._
import java.net.InetSocketAddress
import akka.actor.ActorSystem
import akka.NotUsed
import akka.http.scaladsl.{ClientTransport, Http}
import akka.http.scaladsl.settings.{ClientConnectionSettings, ConnectionPoolSettings}
import akka.http.scaladsl.model.ws._
import akka.stream.scaladsl._
import scala.util.{Try, Success, Failure}

object Entry extends App {
  implicit val config: Config = ConfigFactory.load()
  val clusterName = config.getString("application.cluster-name")

  val localhost: InetAddress = InetAddress.getLocalHost
  lazy val nodeId = Option(localhost.getHostName) filterNot (_.isEmpty) getOrElse localhost.getHostAddress
  lazy val cores = Runtime.getRuntime.availableProcessors

  implicit val log = LoggerFactory.getLogger("app")

  implicit val system = ActorSystem("system")
  implicit val mat = Materializer(system)
  implicit val executionContext = system.dispatchers.defaultGlobalDispatcher

  val proxyHost = "137.136.51.120"
  val proxyPort = 9090

  val httpsProxyTransport = ClientTransport.httpsProxy(InetSocketAddress.createUnresolved(proxyHost, proxyPort))

  val settings = ConnectionPoolSettings(system)
    .withConnectionSettings(ClientConnectionSettings(system)
      .withTransport(httpsProxyTransport))

  val kafkaAvroSer = Kafka_Streams_Core.KafkaSettingsAvroSerWithProducer(config, system)

  log.info(s"'${config.getString("application.name")}', version ${config.getString("application.version")}, node ${nodeId}, ip ${localhost.getHostAddress}, cpu cores ${cores}")

  var hostsList = List("localhost")
  if (System.getProperty("http.nonProxyHosts") != null && System.getProperty("http.nonProxyHosts").nonEmpty) {
    hostsList = hostsList ++ System.getProperty("http.nonProxyHosts").split('|')
  }
  if (config.getString("proxy.non_proxy_hosts") != null && config.getString("proxy.non_proxy_hosts").nonEmpty) {
    hostsList = hostsList ++ config.getString("proxy.non_proxy_hosts").split(',')
  }
  System.setProperty("http.nonProxyHosts",hostsList.distinct.mkString("|"))

  log.info(s"'${config.getString("application.name")}' configured no proxy hosts for jvm: ${hostsList.distinct.mkString("|")}'")

  for (i <- 1 to 3) {
    val className = f"com.github.magkairatov.Kafka_Flow_$i%03d"  // Include full package name
    Try {
      val clazz = Class.forName(className)

      val ctor = clazz.getDeclaredConstructor(
        classOf[Materializer],
        classOf[org.slf4j.Logger],
        classOf[ActorSystem],
        classOf[Config]
      )

      val flow = ctor.newInstance(mat, log, system, config).asInstanceOf[{ def run(): Unit }]
      flow.run()

      log.info(s"Started $className successfully")
      Thread.sleep(2000)
    } match {
      case Success(_) =>
      case Failure(ex) =>
        log.error(s"Failed to start $className: ${ex.getMessage}", ex)
    }
  }


  for (i <- 19 to 38) {
    val className = f"kz.altyn.bi.msv023.Kafka_Flow_$i%03d"
    Try {
      val clazz = Class.forName(className)

      val ctor = clazz.getDeclaredConstructor(
        classOf[Materializer],
        classOf[org.slf4j.Logger],
        classOf[ActorSystem],
        classOf[Config]
      )

      val flow = ctor.newInstance(mat, log, system, config).asInstanceOf[{ def run(): Unit }]
      flow.run()

      log.info(s"Started $className successfully")
      Thread.sleep(2000)
    } match {
      case Success(_) =>
      case Failure(ex) =>
        log.error(s"Failed to start $className: ${ex.getMessage}", ex)
    }
  }

  for (i <- 40 to 68) {
    val className = f"kz.altyn.bi.msv023.Kafka_Flow_$i%03d"  // Include full package name
    Try {
      val clazz = Class.forName(className)

      val ctor = clazz.getDeclaredConstructor(
        classOf[Materializer],
        classOf[org.slf4j.Logger],
        classOf[ActorSystem],
        classOf[Config]
      )

      val flow = ctor.newInstance(mat, log, system, config).asInstanceOf[{ def run(): Unit }]
      flow.run()

      log.info(s"Started $className successfully")
      Thread.sleep(2000)
    } match {
      case Success(_) =>
      case Failure(ex) =>
        log.error(s"Failed to start $className: ${ex.getMessage}", ex)
    }
  }

  for (i <- 71 to 75) {
    val className = f"kz.altyn.bi.msv023.Kafka_Flow_$i%03d"  // Include full package name
    Try {
      val clazz = Class.forName(className)

      val ctor = clazz.getDeclaredConstructor(
        classOf[Materializer],
        classOf[org.slf4j.Logger],
        classOf[ActorSystem],
        classOf[Config]
      )

      val flow = ctor.newInstance(mat, log, system, config).asInstanceOf[{ def run(): Unit }]
      flow.run()

      log.info(s"Started $className successfully")
      Thread.sleep(2000)
    } match {
      case Success(_) =>
      case Failure(ex) =>
        log.error(s"Failed to start $className: ${ex.getMessage}", ex)
    }
  }

  for (i <- 77 to 78) {
    val className = f"kz.altyn.bi.msv023.Kafka_Flow_$i%03d"  // Include full package name
    Try {
      val clazz = Class.forName(className)

      val ctor = clazz.getDeclaredConstructor(
        classOf[Materializer],
        classOf[org.slf4j.Logger],
        classOf[ActorSystem],
        classOf[Config]
      )

      val flow = ctor.newInstance(mat, log, system, config).asInstanceOf[{ def run(): Unit }]
      flow.run()

      log.info(s"Started $className successfully")
      Thread.sleep(2000)
    } match {
      case Success(_) =>
      case Failure(ex) =>
        log.error(s"Failed to start $className: ${ex.getMessage}", ex)
    }
  }

  val cluster_log = LoggerFactory.getLogger("cluster")

  Await.result(system.whenTerminated, Duration.Inf)

  log.info(s"${clusterName} is terminated")
}
