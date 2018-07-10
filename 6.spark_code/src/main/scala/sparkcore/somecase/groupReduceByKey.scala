package sparkcore.somecase

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: shawn pross
  * Date: 2018/04/25
  * Description: 
  */
object  groupReduceByKey{
	val conf = new SparkConf().setMaster("local").setAppName("groupByCount")
	val sc = new SparkContext(conf)

	def main(args: Array[String]): Unit = {

		val array = Array(("hello", "word"), ("hello", "scala"), ("hi", "word"), ("hi", "scala"))
		val rdd = sc.parallelize(array)
		//		val unit = rdd.reduceByKey((a,b)=>a+b).collect()
		//		unit.foreach(x=>print(x._1,x._2))
		val unit = rdd.groupByKey()
		val tuples = unit.map(tuple => (
//			val key = tuple._1
//			var word = ""
//			while (tuple._2.iterator.hasNext) {
//				word = tuple._2.iterator.next()
//			}
//			(key, word)
				tuple._1,tuple._2.mkString(" ")
		)).collect().foreach(x=>print(x))
	}
}
