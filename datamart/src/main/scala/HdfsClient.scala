import com.typesafe.scalalogging.Logger

import java.io._
import java.net.{HttpURLConnection, URI, URISyntaxException}
import scala.util.Using

class HdfsClient(
                  private val user: String,
                  private val downloadOrigin: String,
                  private val uploadOrigin: String,
                  private val namenodeRpcAddress: String,
                ) {

  private val hdfsBaseDownloadUrl = downloadOrigin + "/webhdfs/v1/user/"
  private val hdfsBaseUploadUrl = uploadOrigin + "/webhdfs/v1/user/"
  private val log = Logger("HdfsClient")

  @throws(classOf[IOException])
  @throws(classOf[URISyntaxException])
  def download(hdfsFilePath: String, localDownloadPath: String): Boolean = {
    val uri = new URI(
      "" +
        hdfsBaseDownloadUrl +
        user + "/" +
        hdfsFilePath +
        "?op=OPEN"
    )
    val connection = uri.toURL.openConnection.asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("GET")

    Using(connection.getInputStream) { is =>
      Using(new FileOutputStream(localDownloadPath)) { fos =>
        is.transferTo(fos)
      }
    }

    val responseCode = connection.getResponseCode
    if (responseCode == HttpURLConnection.HTTP_OK) {
      log.info("File {} downloaded successfully from HDFS.", hdfsFilePath)
      true
    } else {
      log.info("Failed to download file from HDFS (HTTP Status: " + responseCode + ")")
      false
    }
  }

  @throws(classOf[IOException])
  @throws(classOf[URISyntaxException])
  def upload(localFilePath: String, hdfsUploadPath: String): Boolean = {
    val uploadUri = "" +
      hdfsBaseUploadUrl +
      user + "/" +
      hdfsUploadPath +
      "?op=CREATE&user.name=" + user +
      "&namenoderpcaddress=" + namenodeRpcAddress +
      "&createflag=&createparent=true&overwrite=true"
    log.info("Upload uri {}", uploadUri)
    val uri = new URI(uploadUri)
    val connection = uri.toURL.openConnection.asInstanceOf[HttpURLConnection]
    connection.setDoOutput(true)
    connection.setRequestMethod("PUT")

    Using(connection.getOutputStream) { os =>
      Using(new FileInputStream(localFilePath)) { fis =>
        fis.transferTo(os)
      }
    }

    val responseCode = connection.getResponseCode
    if (responseCode == HttpURLConnection.HTTP_CREATED) {
      log.info("File {} uploaded successfully to HDFS.", localFilePath)
      true
    } else {
      log.info("Failed to upload file to HDFS (HTTP Status: " + responseCode + ")")
      false
    }
  }

}
