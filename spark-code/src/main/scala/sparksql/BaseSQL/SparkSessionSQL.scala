package sparksql.BaseSQL

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: shawn pross
  * Date: 2018/05/03
  * Description:
  *  RDD => DataFrame
  *  in spark2.x
  */
object SparkSessionSQL {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setMaster("local").setAppName("SQLContext")
		val sc = new SparkContext(conf)

		val spark = SparkSession.builder().appName("Spark SQL basic exmple").config("spark.some.config.option","some-value").getOrCreate()
		val df = spark.read.json("E:\\BigData\\14_spark\\day08SQL\\资料\\resources\\people.json")
		df.show()

	}
}
