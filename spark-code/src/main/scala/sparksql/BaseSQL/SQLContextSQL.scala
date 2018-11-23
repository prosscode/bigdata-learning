package sparksql.BaseSQL

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: shawn pross
  * Date: 2018/05/03
  * Description:
  * 	in spark1.x
  */
object SQLContextSQL {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setMaster("local").setAppName("SQLContextSQL")
		val sc = new SparkContext(conf)
		// SQLContext是spark1.x使用的api
		val sqlContext: SQLContext = new SQLContext(sc)

		/**
		  * 加载数据
		  * 数据源：
		  * 	1.hive里面的表
		  * 	2.关系型数据库里面的表
		  * 	3.结构化文件（json，etc.）
		  * 	4.已经存在的RDD
		  * 	5.Parquet文件格式的数据（sparkSQL对这个文件支持最好）
		  */

		//方式一
		val df1: DataFrame = sqlContext.read.json("E:\\BigData\\14_spark\\day08SQL\\资料\\resources\\people.json")
		val df1s = sqlContext.read.parquet("E:\\BigData\\14_spark\\day08SQL\\资料\\resources\\users.parquet")
//		df1.show()
//		df1s.show()

		//方式二
		val df2 = sqlContext.read.format("json").load("E:\\BigData\\14_spark\\day08SQL\\资料\\resources\\people.json")
		val df2s = sqlContext.read.format("text").load("E:\\BigData\\14_spark\\day08SQL\\资料\\resources\\people.txt")
		df2s.show()

		//方式三，不指定，默认为Parquet文件格式
//		sqlContext.load("")


		/**
		  * 读数据
		  */
		//方式一
		df2.write.json("")
		df2.write.parquet("")
		//方式二，mode模式
		df2.write.format("json").save("hdfs://hadoop1:9000/result")
		df2.write.format("parquet").mode(SaveMode.Ignore).save("path")
		//方式三，不指定文件格式，默认为parquet格式
		df2.write.save("path")
	}
}
