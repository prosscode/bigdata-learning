package com.aura.category;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;

public class CategoryRealCount {
    public static void main(String[] args) {
        //初始化程序入口
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("CategoryRealCount");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(3));

        //读取数据
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list","192.168.123.102:9092");
        HashSet<String> topics = new HashSet<String>();
        topics.add("aura");
        JavaDStream<String> logDStream = KafkaUtils.createDirectStream(
                ssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics
        ).map(new Function<Tuple2<String, String>, String>() {
            public String call(Tuple2<String, String> tuple2) throws Exception {
                return tuple2._2;
            }
        });
        logDStream.print();

        //代码的逻辑
        //启动应用程序
        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        ssc.stop();
    }
}
