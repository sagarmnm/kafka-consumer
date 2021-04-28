package com.sg.spafka.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SGSparkProcessor implements Serializable {
    static JavaStreamingContext streamingContext = new JavaStreamingContext(new SparkConf().setAppName("SGSparkProcessor").setMaster("local[2]"), Durations.seconds(10));

    public void mapToAsciis() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "broker:29092");
        kafkaParams.put("key.deserializer", LongDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "stream1");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", true);

        final JavaInputDStream<ConsumerRecord<Long, String>> javaInputDStream =
                KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(Collections.singletonList("sgtopic"), kafkaParams));

        javaInputDStream.foreachRDD(rddInInputStream -> {
            if (!rddInInputStream.isEmpty()) {
                OffsetRange[] offsetRanges = ((HasOffsetRanges) rddInInputStream.rdd()).offsetRanges();
                JavaRDD<ConsumerRecord<Long, String>> javaRDD =
                        KafkaUtils.createRDD(streamingContext.sparkContext(), kafkaParams, offsetRanges,
                                LocationStrategies.PreferConsistent());
                javaRDD.foreach((VoidFunction<ConsumerRecord<Long, String>>) consumerRecord -> System.out.println("SG upper case value is: " + consumerRecord.value().toUpperCase()));
            }
        });
        try {
            streamingContext.start();
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}