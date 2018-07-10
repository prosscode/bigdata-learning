package sparkcore.TransfOP;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Author: shawn pross
 * Date: 2018/04/23
 * Description:  mapPartitions
 */
public class MapPartitiosOP {
    final static SparkConf conf = new SparkConf().setAppName("RDDOP").setMaster("local");
    final static JavaSparkContext sc = new JavaSparkContext(conf);

    public static void mapPar(){
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> rdd = sc.parallelize(list, 2);
//        System.out.print(rdd);
        rdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer.intValue());
            }
        });
    }
    public static void main(String[] args){
        mapPar();
    }
}
