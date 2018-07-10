package sparkcore.lesson01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by Administrator on 2018/4/20.
 */
public class WordCount7 {
    public static void main(String[] args) {
        //初始化程序入口
        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("WordCount7");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final JavaRDD<String> fileRDD = sc.textFile("D:\\1711spark\\src\\hello.txt");
        //算子flatMap
        final JavaRDD<String> wordRDD = fileRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(",")).iterator();
            }
        });

        final JavaPairRDD<String, Integer> wordOneRDD = wordRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });
        //sortBykey groupbykey reducebykey
        final JavaPairRDD<String, Integer> wordCountRDD = wordOneRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });

        final JavaPairRDD<Integer, String> count2wordRDD = wordCountRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                return new Tuple2<Integer, String>(tuple._2, tuple._1);
            }
        });

        final JavaPairRDD<Integer, String> sortedRDD = count2wordRDD.sortByKey(false);
        sortedRDD.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> tuple) throws Exception {
                System.out.println("单词 "+tuple._2 + " 次数："+tuple._1);
            }
        });

        sc.stop();

    }
}
