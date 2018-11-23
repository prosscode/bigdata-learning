package sparkcore.CountCase

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: shawn pross
  * Date: 2018/04/24
  * Description: 
  */
object LineCountScala {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
				.setAppName("LineCount")
				.setMaster("local")
		val sc = new SparkContext(conf);
		val lines = sc.textFile("test.txt", 1)
		val pairs = lines.map { (_, 1) }
		val lineCounts = pairs.reduceByKey { _ + _ }
		lineCounts.foreach(lineCount => println(lineCount._1 + " : " + lineCount._2 ))

	}
}
