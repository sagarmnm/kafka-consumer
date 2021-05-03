package com.sg.spark.controller;

import com.sg.spark.utils.SGKafkaConsumer;
import lombok.extern.log4j.Log4j2;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Log4j2
public class SparkController {
    @PostMapping("/kafka-outbound/message")
    public String processKafkaOutboundMessage() {
        SGKafkaConsumer sgKafkaConsumer = new SGKafkaConsumer();
        sgKafkaConsumer.receiveOutboundKafkaMessage(null);
        return "";
    }
}
