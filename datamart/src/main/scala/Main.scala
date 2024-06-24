import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

object Main {
  private val PORT = 9001

  private val datamart = DataMart

  def main(args: Array[String]): Unit = {
    startHttpServer()
  }

  private def startHttpServer(): Unit = {
    implicit val system: ActorSystem = ActorSystem("datamart-server")

    val route: Route = path("preprocess") {
      get {
        datamart.preprocessDataset()
        complete("Dataset is preprocessed and loaded to HDFS")
      }
    }

    Http().newServerAt("localhost", PORT).bind(route)
  }
}
