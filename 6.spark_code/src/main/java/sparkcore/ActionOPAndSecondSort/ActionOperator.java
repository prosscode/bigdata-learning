package sparkcore.ActionOPAndSecondSort;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Author: shawn pross
 * Date: 2018/04/24
 * Description:
 */
public class ActionOperator {
    static SparkConf conf=new SparkConf().setMaster("local").setAppName("transformation");
    static JavaSparkContext sc=new JavaSparkContext(conf);

    /**
     * top2 of word time
     */
    public static void topN() {
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("you jump", "i jump", "you jump"));
        JavaPairRDD<String, Integer> pairRDD = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String word) throws Exception {
                return Arrays.asList(word.split(" ")).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String str) throws Exception {
                return new Tuple2<String, Integer>(str, 1);
            }
        });

    }

    public static void main(String[] args) {

    }
}
