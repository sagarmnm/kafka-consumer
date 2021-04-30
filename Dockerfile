FROM java:8
WORKDIR /target
ADD target/kafka-consumer-0.0.1-SNAPSHOT.jar kafka-consumer-0.0.1-SNAPSHOT.jar
EXPOSE 8002
ENTRYPOINT ["java", "-jar", "kafka-consumer-0.0.1-SNAPSHOT.jar"]