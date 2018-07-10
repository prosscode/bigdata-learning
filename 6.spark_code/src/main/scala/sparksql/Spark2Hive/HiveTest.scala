package sparksql.Spark2Hive

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: shawn pross
  * Date: 2018/05/03
  * Description: 
  */
object HiveTest {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setMaster("local").setAppName("test")
		val sc = new SparkContext(conf)
//		val sqlContext = new HiveContext(sc)
//		sqlContext.sql("select count(*) from log").show()
	}
}
