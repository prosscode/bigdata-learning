package com.dmp.tags

import com.dmp.entity.Logs
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
  * Author: shawn pross
  * Date: 2018/05/15
  * Description:  合并Tag标签
  */
object MergeTag {
	def main(args: Array[String]): Unit = {
		/**
		  * 1) 判断参数个数
		  */
		if (args.length < 2) {
			println(
				"""
				  |com.dmp.tags.MergeTag
				  | <inputPath> 日志目录
				  | <appPath> app映射文件路径
				  | <devicePath> 设备的映射文件路径
				  | <outputPath> 输出的结果文件存储
				""".stripMargin)
			System.exit(0)
		}
		/**
		  * 2）接收参数
		  */
		val Array(inputPath,appPath,devicePath,outputPath) = args
		/**
		  * 3) 初始化程序入口
		  */
		val conf = new SparkConf()
		conf.setAppName(s"${this.getClass.getSimpleName}")
		conf.setMaster("local")

		//Kryo序列化
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		conf.registerKryoClasses(Array(classOf[Logs]))
		val sc = new SparkContext(conf)

		//生成 APP 广播 变量
		val appMap: Map[String, String] = sc.textFile(appPath).flatMap(line => {
			var map = Map[String, String]()
			val fields = line.split("\t")
			if (fields.length > 4) {
				map += (fields(4) -> fields(1))
			}
			map
		}).collect().toMap

		sc.textFile(inputPath).map(line => {
			val logs: Logs = Logs.line2Log(line)
			/**
			  * 取出每一个logs中的对应的值，打上标签
			  * 转换为List，进行合并
			  */
			val advTypeTag: mutable.Map[String, Int] = Tagging.advTypeTag(logs)
			val appName: mutable.Map[String, Int] = Tagging.appNameTag(logs)
			val channelTag: mutable.Map[String, Int] = Tagging.channelTag(logs)
			val keywordTag: mutable.Map[String, Int] = Tagging.keywordTag(logs)
			val locationTag: Array[mutable.Map[String, Int]] = Tagging.locationTag(logs)
			val id: String = Tagging.getNotEmptyID(logs).getOrElse("")

			(id, (advTypeTag ++ appName ++ channelTag ++ keywordTag ++ locationTag).toList)
		})
//				.filter(!_._1.toString.equals("")).reduceByKey {
//			case (list1, list2) => {(list1 ++ list2).groupBy(_._1)
//					.map {
//						case (k, list) => {
//							(k, list.map(t => t._2).sum)
//						}
//					}.toList
//			}
//		}.foreach(tuple => {
//			println(tuple._1 + "->" + tuple._2.mkString("\t"))
//		})

		sc.stop()
	}
}
