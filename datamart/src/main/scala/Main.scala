import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

import scala.concurrent.duration.DurationInt

object Main {
  private val PORT = 9001

  private val datamart = DataMart

  //  add to VM options
  //  --add-exports java.base/sun.nio.ch=ALL-UNNAMED
  def main(args: Array[String]): Unit = {
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

    Http().newServerAt("localhost", PORT).bind(route)
  }
}
