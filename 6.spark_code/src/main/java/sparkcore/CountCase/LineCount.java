package sparkcore.CountCase;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * Author: shawn pross
 * Date: 2018/04/24
 * Description:
 * The number of occurrences of each line of the file
 */
public class LineCount {
    public static void main(String[] args) {
        //create sparkconf
        SparkConf conf = new SparkConf().setAppName("LineCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //create init rdd from local text file
        JavaRDD<String> javaRDD = sc.textFile("E:\\BigData\\14_spark\\day01\\debug.log");
        //for line => mapToPair => (line,1)
        JavaPairRDD<String, Integer> pairRDD = javaRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        // pairRDD => reduceByKey => value
        JavaPairRDD<String, Integer> reduceByKeyRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        //reduceByKeyRDD => foreach => result
        reduceByKeyRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println(tuple._1+"->"+tuple._2);
            }
        });

        //close SparkContext
        sc.close();
    }

}
