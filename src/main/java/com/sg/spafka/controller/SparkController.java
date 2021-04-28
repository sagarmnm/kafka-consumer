package com.sg.spafka.controller;

import com.sg.spafka.utils.SGConsumer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SparkController {
    @PostMapping("/kafka-outbound/message")
    public String processKafkaOutboundMessage() {
        SGConsumer sgConsumer = new SGConsumer();
        sgConsumer.receiveOutboundKafkaMessage(null);
        return "";
    }
}
