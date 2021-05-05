package com.sg.spark.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.Arrays;

@Component
public class SGSparkProducer implements Serializable {

    @Value("${spark.appName}")
    private String sparkAppName;

    @Value("${spark.master}")
    private String sparkMaster;

    public void mapToAsciis(String value) {
        SparkConf sc = new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster);
        System.out.println("In mapToAsciis method: " + value);
        // new JavaSparkContext(sc).broadcast(value);
        JavaSparkContext javaSparkContext = new JavaSparkContext(sc);
        JavaRDD<String> splitValueList = javaSparkContext.parallelize(Arrays.asList(value.split(" ")));
        System.out.println("In mapToAsciis method: " + "after Spark parallelize");
        JavaRDD<String> mappedValues = splitValueList.flatMap(
                (FlatMapFunction<String, String>) s -> Arrays.asList(s.toUpperCase()).iterator()
        );
        System.out.println("In mapToAsciis method: " + "Values from spark");
        for (String str : mappedValues.collect()) {
            System.out.println(str);
        }
        javaSparkContext.close();

    }
}