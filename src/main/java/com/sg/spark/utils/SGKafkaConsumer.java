package com.sg.spark.utils;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Component
@Log4j2
public class SGKafkaConsumer implements Serializable {
    @Autowired
    KafkaTemplate<Long, String> kafkaTemplate;

    @Autowired
    SGSparkProducer sparkProducer;

    @KafkaListener(topics = {"sgtopic"})
    public void receiveOutboundKafkaMessage(ConsumerRecord<Long, String> consumerRecord)   {
        log.debug("Received kafka message: " + consumerRecord.value());
        sparkProducer.mapToAsciis(consumerRecord.value());
    }
}
