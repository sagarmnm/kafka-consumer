FROM java:8
WORKDIR /target
ADD target/spafka-consumer-0.0.1-SNAPSHOT.jar spafka-consumer-0.0.1-SNAPSHOT.jar
EXPOSE 8002
ENTRYPOINT ["java", "-jar", "spafka-consumer-0.0.1-SNAPSHOT.jar"]