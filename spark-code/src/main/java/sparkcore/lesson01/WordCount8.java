package sparkcore.lesson01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Administrator on 2018/4/20.
 */
public class WordCount8 {
    public static void main(String[] args) {
        final SparkConf conf = new SparkConf();
        conf.setAppName("WordCount8");
        conf.setMaster("local");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        /////////////////////////////
//        final JavaRDD<String> fileRDD = sc.textFile("D:\\1711spark\\src\\hello.txt");
//        final JavaRDD<String> wordRDD = fileRDD.flatMap(line ->
//                Arrays.asList(line.split(",")).iterator());
//        final JavaPairRDD<String, Integer> wordOneRDD = wordRDD.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
//        final JavaPairRDD<String, Integer> wordCountRDD = wordOneRDD.reduceByKey((m, n) -> m + n);
//        final JavaPairRDD<Object, Object> count2wordRDD = wordCountRDD.mapToPair(tuple -> new Tuple2(tuple._2, tuple._1));
//        final JavaPairRDD<Object, Object> sortedRDD = count2wordRDD.sortByKey(false);
//        sortedRDD.foreach( tuple -> {
//            System.out.println("单词 "+tuple._2 + " 次数："+ tuple._1());
//        });

        ////////////////////////////////////////////////////////
//        final List<Tuple2<Object, Object>> result = sc.textFile("D:\\1711spark\\src\\hello.txt")
//                .flatMap(line -> Arrays.asList(line.split(",")).iterator())
//                .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
//                .reduceByKey((m, n) -> m + n)
//                .mapToPair(tuple -> new Tuple2(tuple._2, tuple._1))
//                .sortByKey(false)
//                .take(2);
//
//        result.forEach( t -> {
//            System.out.println(t._2() + "  "+ t._1());
//        });


              sc.textFile("D:\\1711spark\\src\\hello.txt")
                      .flatMap(line -> Arrays.asList(line.split(",")).iterator())
                      .mapToPair(word -> new Tuple2<String, Integer>(word, 1)).reduceByKey((m, n) -> m + n)
                      .mapToPair(tuple -> new Tuple2<Integer,String>(tuple._2, tuple._1))
                      .sortByKey(false).foreach( tuple -> {
                        System.out.println("单词 "+tuple._2 + " 次数："+ tuple._1());
        });






    }
}
