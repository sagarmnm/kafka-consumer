package com.sg.spark.controller;

import com.sg.spark.utils.SGKafkaConsumer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SparkController {
    @PostMapping("/kafka-outbound/message")
    public String processKafkaOutboundMessage() {
        SGKafkaConsumer sgKafkaConsumer = new SGKafkaConsumer();
        sgKafkaConsumer.receiveOutboundKafkaMessage(null);
        return "";
    }
}
