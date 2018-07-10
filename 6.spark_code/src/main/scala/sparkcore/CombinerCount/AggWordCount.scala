package sparkcore.CombinerCount

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: shawn pross
  * Date: 2018/04/24
  * Description: 先局部聚合，然后全局聚合
  */
object AggWordCount {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setMaster("local").setAppName("AggWordCount")
		val sc = new SparkContext(conf)
		val array = Array("you,jump","jump,jump","jump,jump","jump,jump","jump,jump","jump,jump","jump,jump")
		val rdd = sc.parallelize(array)
		//局部聚合
		val value1 = rdd.flatMap(_.split(",")).map(word => {
			val prefix = (new util.Random()).nextInt(3)
			(prefix + "_" + word, 1)
		}).reduceByKey(_ + _)
		//遍历增加"_"后的key的样式
		value1.foreach(tuple=>println(tuple._1,tuple._2))

		//全局聚合
		val value2 = value1.map(tuple => {
			val key = tuple._1.split("_")(1)
			(key, tuple._2)
		}).groupByKey().map(tuple => (tuple._1, tuple._2.sum))
		//遍历最后的样式
		value2.foreach(tuple=>println(tuple._1,tuple._2))

	}


}
