package com.github.magkairatov

import akka.actor.ActorSystem
import akka.stream.{Materializer, Supervision}
import org.slf4j.Logger

import scala.concurrent.Future

trait Flow_Supervision {

  private def scheduleRestart(implicit mat: Materializer, system: ActorSystem, log: Logger) = {
    implicit val ec = mat.executionContext
    Future {
      Thread.sleep(1000)
      log.error(s"System needs restart. Terminating...")
      println(s"System needs restart. Terminating...")
      system.terminate()
    }
  }

  def globalSupervisorException(implicit mat: Materializer, system: ActorSystem, log: Logger): Supervision.Decider = {
    case ex: Exception =>
      log.error(s"Error in stream ${this.getClass.toString} exception occurred and stopping stream: ${ex.getClass} ${ex.getCause} ${ex.getMessage}")
      println(s"Error in stream ${this.getClass.toString} exception occurred and stopping stream: ${ex.getClass} ${ex.getCause} ${ex.getMessage}")
      scheduleRestart
      Supervision.Stop
  }

}
