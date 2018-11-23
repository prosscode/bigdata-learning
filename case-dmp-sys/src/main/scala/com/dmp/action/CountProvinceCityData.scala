package com.dmp.action

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Author: shawn pross
  * Date: 2018/05/14
  * Description: 统计各省市数据量分布情况
  */
object CountProvinceCityData {
	def main(args: Array[String]): Unit = {
		/**
		  * 创建sparksession对象
		  */
		val conf: SparkConf = new SparkConf()
		conf.setAppName("CountProvinceCityData")
		conf.setMaster("local[2]")
		val spark: SparkSession = SparkSession.builder().appName("CountProvinceCityData").config(conf).getOrCreate()

		/**
		  * 读取数据,进行相应的操作
		  */
		val df: DataFrame = spark.read.text("")
		df.createGlobalTempView("logs_count_data")
		val sqlDF: DataFrame = spark.sql("select rtbprovince,rtbcity,count(*) from logs group by rtbprovince,rtbcity")
		sqlDF.show()
		/**
		  * 存入数据库中，使用SaveMode模式，存在表追加数据，不存在先创建表
		  */
		val properties = new Properties()
		properties.put("user","root")
		properties.put("password","root")
		sqlDF.write.mode(SaveMode.Append)
				.jdbc("jdbc:mysql://localhost:3306/spark","logs_count_data",properties)
		spark.stop()

	}

}
