package com.sg.spafka.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Component
public class SGConsumer implements Serializable {
    @Autowired
    KafkaTemplate<Long, String> kafkaTemplate;

    @KafkaListener(topics = {"sgtopic"})
    public void receiveOutboundKafkaMessage(ConsumerRecord<Long, String> consumerRecord)   {
        SGSparkProcessor sgSparkProcessor = new SGSparkProcessor();
        sgSparkProcessor.mapToAsciis();
    }
}
