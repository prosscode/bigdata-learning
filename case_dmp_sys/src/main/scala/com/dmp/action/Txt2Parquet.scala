package com.dmp.action

import com.dmp.entity.Logs
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: shawn pross
  * Date: 2018/05/14
  * Description: txt文件转化为parquet
  */
object Txt2Parquet {
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
		conf.set("spark.io.compression.codec",compressionCode)
		val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

		/**
		  * 读取文件，对文件做对应的操作
		  * 隐式转换
		  */
		import spark.implicits._
		val logDF: DataFrame = spark.sparkContext.textFile(dataPath)
				.map(line=>Logs.line2Log(line)).toDF()
		/**
		  * 指定文件存放的位置
		  */
		logDF.write.parquet(outputPath)
		spark.stop()

	}

}
