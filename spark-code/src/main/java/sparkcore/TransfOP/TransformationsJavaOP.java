package sparkcore.TransfOP;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Author: shawn pross
 * Date: 2018/04/23
 * Description:
 */
public class TransformationsJavaOP {
    static SparkConf conf=new SparkConf().setMaster("local").setAppName("transformation");
    static JavaSparkContext sc=new JavaSparkContext(conf);

    /**
     * map usage
     * add hello to each string
     */
    public static void map(){
        List<String> list = Arrays.asList("pross", "kris", "benny");
        JavaRDD<String> javaRDD = sc.parallelize(list);
        javaRDD.map(new Function<String, String>() {
            @Override
            public String call(String name) throws Exception {
                return "hello "+name;
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String str) throws Exception {
                System.out.println(str);
            }
        });
    }

    /**
     * flatMap usage
     * Split string and output
     */
    public static void flatMap(){
        List<String> list = Arrays.asList("pross kris", "benny error");
        JavaRDD<String> rdd = sc.parallelize(list);
        rdd.flatMap(new FlatMapFunction<String, String>(){
            @Override
            public Iterator<String> call(String names) {
                return Arrays.asList(names.split(" ")).iterator();
            }
        }).map(new Function<String, String>() {
            @Override
            public String call(String names) throws Exception  {
                return "helllo " + names;
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String word) throws Exception {
                System.out.println(word);
            }
        });

    }

    /**
     * join usage
     * meger tupleid,tupleName and tupleScore By Key
     *
     */
    public static void join(){
        List<Tuple2<Integer, String>> names = Arrays.asList(
                new Tuple2<Integer, String>(1, "pross"),
                new Tuple2<Integer, String>(2, "kris"),
                new Tuple2<Integer, String>(3, "benny")
        );
        List<Tuple2<Integer, Integer>> score = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 95)
        );
        JavaPairRDD<Integer, String> namesrdd = sc.parallelizePairs(names);
        JavaPairRDD<Integer, Integer> scorerdd = sc.parallelizePairs(score);

        JavaPairRDD<Integer, Tuple2<String, Integer>> join = namesrdd.join(scorerdd);
        join.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> tuple) throws Exception {
                System.out.println("id->"+tuple._1+",name->"+tuple._2._1+",score->"+tuple._2._2);
            }
        });
    }

    /**
     * union uasge
     * meger list elements
     */
    public static void union(){
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        List<Integer> list1 = Arrays.asList(7, 8, 9, 10);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        JavaRDD<Integer> list1RDD = sc.parallelize(list1);
        JavaRDD<Integer> union = listRDD.union(list1RDD);
        union.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer+",");
            }
        });

    }

    /**
     * mapPartitions uage
     *  获取的是一个分区的数据（HDFS）
     *  mapPartitions是一个高性能的算子
     *  因为每次处理的是一个分区的数据，减少了去获取数据的次数
     */
    public static void mapPartitions(){
        List<String> list = Arrays.asList("pross","kris","tommy","benny");
        JavaRDD<String> javaRDD = sc.parallelize(list, 2);
        javaRDD.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            //每次处理的是一个分区的数据
            @Override
            public Iterator<String> call(Iterator<String> iterator) throws Exception {
                //定义List去接收遍历出来的字符串
                ArrayList<String> arrayList = new ArrayList<>();
                while(iterator.hasNext()){
                    arrayList.add("hello,"+iterator.next());
                }
                //把List变成Iterator来返回出去
                return arrayList.iterator();
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

    }

    public static void main(String[] args) {
//        flatMap();
//        join();
//        union();
        mapPartitions();
    }

}
