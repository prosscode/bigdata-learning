package com.dmp.action

import com.dmp.entity.Logs
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Author: shawn pross
  * Date: 2018/05/14
  * Description: 报表 地域分布
  */
object LocationDistributed {
	def main(args: Array[String]): Unit = {
		/**
		  * 判断args中参数是否符合要求
		  * 原始文件路径、输出文件路径、压缩格式
		  */
		if(args.length<3){
			print(
				"""
				  |<dataPath>：日志所在的路径
				  |<outputPath>:结果文件存放的路径
				  |<compressionCode>:指定的压缩格式
				  |>
				""".stripMargin)
			System.exit(0)
		}

		/**
		  * 接收参数
		  */
		val Array(dataPath,outputPath,compressionCode)=args
		/**
		  * 创建sparksession对象
		  */
		val conf: SparkConf = new SparkConf()
		conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
		conf.registerKryoClasses(Array(classOf[Logs]))
		val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

		/**
		  * 读取文件,进行相应的操作
		  */
		import spark.implicits._
		val df = spark.sparkContext.textFile(dataPath).toDF()




	}
}
