FROM ubuntu:kinetic-20230412

RUN apt-get update --yes && \
    apt-get install --yes openjdk-19-jdk

WORKDIR /build
COPY gradle/wrapper/ ./gradle/wrapper/
COPY settings.gradle build.gradle ./
COPY --chmod=744 gradlew ./

RUN mkdir -p src/main/java/de/claasklar && \
    echo "package de.claasklar; public class Main {public static void main(String[] args){}}" && \
    ./gradlew build && \
    rm -r src

COPY src ./src
RUN ./gradlew jar

CMD ["java", "-jar", "build/libs/ycsb-squared-1.0-SNAPSHOT.jar", "-Dio.opentelemetry.context.enableStrictContext=true"]
