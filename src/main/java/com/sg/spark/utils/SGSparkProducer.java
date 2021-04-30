package com.sg.spark.utils;

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
public class SGSparkProducer implements Serializable {

    @Value("${spark.appName}")
    private String sparkAppName;

    @Value("${spark.master}")
    private String sparkMaster;

    public void mapToAsciis(String value) {
        SparkConf sc = new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster);
        System.out.println("in map to asciis");
        // new JavaSparkContext(sc).broadcast(value);
        JavaSparkContext javaSparkContext = new JavaSparkContext(sc);
        JavaRDD<String> splitValueList = javaSparkContext.parallelize(Arrays.asList(value.split(" ")));
        System.out.println("************** after parallelize");
        JavaRDD<String> mappedValues = splitValueList.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String s){
                        return Arrays.asList(s.toUpperCase()).iterator();
                    }

                }
        );
        System.out.println("************** " + mappedValues.count());
        mappedValues.collect().forEach(System.out::println);
        javaSparkContext.close();

    }
}