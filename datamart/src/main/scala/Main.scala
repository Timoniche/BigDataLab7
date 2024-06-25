import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.Logger

import scala.concurrent.duration.DurationInt

object Main {
  private val PORT = 9001

  private val datamart = DataMart
  private val logger = Logger("MainServerApp")

  //  add to VM options
  //  --add-exports java.base/sun.nio.ch=ALL-UNNAMED
  def main(args: Array[String]): Unit = {
    logger.info("Starting Datamarket http Server")
    startHttpServer()
  }

  private def startHttpServer(): Unit = {
    implicit val system: ActorSystem = ActorSystem("datamart-server")

    val route: Route = path("preprocess") {
      withRequestTimeout(5.minute) {
        get {
          datamart.preprocessDataset()
          complete("Dataset is preprocessed and loaded to HDFS")
        }
      }
    }

    Http().newServerAt("0.0.0.0", PORT).bind(route)
    logger.info("Server is online on port: {}", PORT)
  }
}
