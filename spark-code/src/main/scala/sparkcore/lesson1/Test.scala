package sparkcore.lesson1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/4/20.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCount")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)
    val array = Array("you,jump","i,jump")
    val list = List(1,2,3,4)
//    val rdd: RDD[Int] = sc.parallelize(list)
//    val count = rdd.count()
//    println(count)

    val rdd: RDD[Int] = sc.makeRDD(list)
    val count = rdd.count()

  }

}
