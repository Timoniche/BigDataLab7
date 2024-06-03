import com.typesafe.scalalogging.Logger

import java.io._
import java.net.{HttpURLConnection, URI, URISyntaxException}

class HdfsClient {

  private val hdfsBaseUrl = "http://127.0.0.1:9870/webhdfs/v1"
  private val log = Logger("Logger")


  def download(hdfsFilePath: String, localDownloadPath: String): Unit = {
    try {
      val uri = new URI(hdfsBaseUrl + hdfsFilePath + "?op=OPEN")
      val connection = uri.toURL.openConnection.asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("GET")

      val is = connection.getInputStream
      val fos = new FileOutputStream(localDownloadPath)
      is.transferTo(fos)

      val responseCode = connection.getResponseCode
      if (responseCode == HttpURLConnection.HTTP_OK) {
        log.info("File downloaded successfully from HDFS.")
      } else {
        log.info("Failed to download file from HDFS (HTTP Status: " + responseCode + ")")
      }
    } catch {
      case ex@(_: IOException | _: URISyntaxException) => log.info("download ex: {}", ex.getMessage)
    }
  }

  def upload(localFilePath: String, hdfsDownloadPath: String): Unit = {
    try {
      val uri = new URI(hdfsBaseUrl + hdfsDownloadPath + "?op=CREATE")
      val connection = uri.toURL.openConnection.asInstanceOf[HttpURLConnection]
      connection.setDoOutput(true)
      connection.setRequestMethod("PUT")

      val os = connection.getOutputStream
      val fis = new FileInputStream(localFilePath)
      fis.transferTo(os)

      val responseCode = connection.getResponseCode
      if (responseCode == HttpURLConnection.HTTP_CREATED) {
        log.info("File uploaded successfully to HDFS.")
      } else {
        log.info("Failed to upload file to HDFS (HTTP Status: " + responseCode + ")")
      }
    } catch {
      case ex@(_: IOException | _: URISyntaxException) => log.info("upload ex: {}", ex.getMessage)
    }
  }

}

//object Main {
//  def main(args: Array[String]): Unit = {
//    val curDir = "/Users/timoniche/Documents/BigData/BigDataLab7/datamart/src/main/scala"
//
//    val hdfsFilePath = "/user/ddulaev/dataset.csv"
//    val localDownloadPath = curDir + "/dataset.csv"
//
//    new HdfsClient().download(hdfsFilePath, localDownloadPath)
//  }
//}
