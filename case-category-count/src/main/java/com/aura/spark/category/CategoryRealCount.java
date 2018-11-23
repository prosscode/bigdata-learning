package com.aura.spark.category;

import com.aura.dao.HBaseDao;
import com.aura.dao.factory.HBaseFactory;
import com.aura.utils.Utils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class CategoryRealCount {
    public static void main(String[] args) {
        //初始化程序入口
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("CategoryRealCount");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(3));


        //读取数据
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list","hadoop1:9092");
        HashSet<String> topics = new HashSet<String>();
        topics.add("aura");
        KafkaUtils.createDirectStream(
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
        }).mapToPair(new PairFunction<String, String, Long>() {
            public Tuple2<String, Long> call(String line) throws Exception {
                return new Tuple2<String, Long>(Utils.getKey(line),1L);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            public Long call(Long x, Long y) throws Exception {
                return x+y;
            }
        }).foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    public void call(Iterator<Tuple2<String, Long>> partition) throws Exception {
                        HBaseDao hBaseDao = HBaseFactory.getHBaseDao();
                        while(partition.hasNext()){
                            Tuple2<String, Long> tuple = partition.next();
                            hBaseDao.save("aura",tuple._1,"f","name",tuple._2);
                           System.out.println(tuple._1 + "   "+ tuple._2);
                        }
                    }
                });
            }
        });
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
