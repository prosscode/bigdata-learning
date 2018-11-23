package sparkcore.day1.lesson1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/4/20.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    /***
      * spark-shell:
      *   spark： SparkSession 主要针对的是SparkSQL:
      *                        SparkSQL程序入口
      *   sc:  SparkCore对象，SparkCore的程序入口
      */
      val conf = new SparkConf()
      //如果这个参数不设置，默认认为你运行的是集群模式
      //如果设置成local代表运行的是local模式
      //conf.setMaster("local")
      //设置任务名
      conf.setAppName("WordCount")
       //创建SparkCore的程序入口
      val sc = new SparkContext(conf)
    //读取文件 生成RDD
      val fileRDD: RDD[String] = sc.textFile("hdfs://hadoop1:9000/hello.txt")
    //把每一行数据按照，分割
      val wordRDD: RDD[String] = fileRDD.flatMap(line => line.split(","))
    //让每一个单词都出现一次
      val wordOneRDD: RDD[(String, Int)] = wordRDD.map(word => (word,1))
    //单词计数
      val wordCountRDD: RDD[(String, Int)] = wordOneRDD.reduceByKey(_+_)
    //按照单词出现的次数 降序排序
      val sortedRDD = wordCountRDD.sortBy( tuple => tuple._2,false)

    sortedRDD.saveAsTextFile("hdfs://hadoop1:9000/result")

//     //打印结果
//    sortedRDD.foreach( tuple =>{
//      println("单词："+tuple._1 + " 出现的次数"+tuple._2)
//    })

//    //流式编程，函数式编程 （pig） Trident
//    sc.textFile("D:\\1711spark\\src\\hello.txt")
//      .flatMap(_.split(","))
//      .map((_,1))
//      .reduceByKey(_+_)
//      .sortBy(_._2)
//      .foreach( tuple =>{
//        print(tuple._1 + " "+ tuple._2)
//      })



    sc.stop()

  }

}
