FROM sbtscala/scala-sbt:eclipse-temurin-17.0.4_1.7.1_3.2.0

WORKDIR /app

ENV JAVA_TOOL_OPTIONS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"

COPY . /app

CMD ["sbt", "compile"]
CMD ["sbt", "runMain Main"]
