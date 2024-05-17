package hdfs;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

public class HdfsClient {

    private final String hdfsBaseUrl = "http://127.0.0.1:9870/webhdfs/v1";

    public HdfsClient() {
    }

    public void download(
            String hdfsFilePath,
            String localDownloadPath
    ) {
        try {
            URI uri = new URI(hdfsBaseUrl + hdfsFilePath + "?op=OPEN");
            HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
            connection.setRequestMethod("GET");

            try (InputStream is = connection.getInputStream(); FileOutputStream fos = new FileOutputStream(localDownloadPath)) {
                is.transferTo(fos);
            }

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                System.out.println("File downloaded successfully from HDFS.");
            } else {
                System.out.println("Failed to download file from HDFS (HTTP Status: " + responseCode + ")");
            }

        } catch (IOException | URISyntaxException ex) {
            System.out.println(ex.getMessage());
        }
    }

    //todo: write to hdfs api

    public static void main(String[] args) {
        String hdfsFilePath = "/user/ddulaev/dataset.csv";
        String rootPath = Paths.get(".").toAbsolutePath().normalize().toString();
        String localDownloadPath = rootPath + "/src/dataset.csv";

        new HdfsClient().download(hdfsFilePath, localDownloadPath);
    }
}
