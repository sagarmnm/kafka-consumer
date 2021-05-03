package com.sg.spark.utils;

import lombok.extern.log4j.Log4j2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Component
@Log4j2
public class SGSparkProducer implements Serializable {

    @Value("${spark.appName}")
    private String sparkAppName;

    @Value("${spark.master}")
    private String sparkMaster;

    public void mapToAsciis(String value) {
        SparkConf sc = new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster);
        log.debug("mapToAsciis method: " + value);
        // new JavaSparkContext(sc).broadcast(value);
        JavaSparkContext javaSparkContext = new JavaSparkContext(sc);
        JavaRDD<String> splitValueList = javaSparkContext.parallelize(Arrays.asList(value.split(" ")));
        log.debug("mapToAsciis method: " + "after Spark parallelize");
        JavaRDD<String> mappedValues = splitValueList.flatMap(
                (FlatMapFunction<String, String>) s -> Arrays.asList(s.toUpperCase()).iterator()
        );
        for (String str : mappedValues.collect()) {
            log.debug(str);
        }
        javaSparkContext.close();

    }
}