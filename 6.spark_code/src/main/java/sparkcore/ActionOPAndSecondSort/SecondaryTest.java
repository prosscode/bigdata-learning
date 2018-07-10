package sparkcore.ActionOPAndSecondSort;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * Author: shawn pross
 * Date: 2018/04/24
 * Description:
 */
public class SecondaryTest {
    static SparkConf conf=new SparkConf().setMaster("local").setAppName("transformation");
    static JavaSparkContext sc=new JavaSparkContext(conf);

    public static void main(String[] args) {

        JavaRDD<String> rdd = sc.textFile("E:\\BigData\\14_spark\\day03\\sort.txt");
        rdd.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            @Override
            public Tuple2<SecondarySortKey, String> call(String line) throws Exception {
                String[] split = line.split(",");
                SecondarySortKey sortKey = new SecondarySortKey(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
                return new Tuple2<>(sortKey,line);
            }
        }).sortByKey(true).foreach(new VoidFunction<Tuple2<SecondarySortKey, String>>() {
            @Override
            public void call(Tuple2<SecondarySortKey, String> tuple) throws Exception {
                System.out.println(tuple._2);
            }
        });
    }


}
