package com.dmp.action

import com.dmp.entity.Logs
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Author: shawn pross
  * Date: 2018/05/14
  * Description:  报表 终端设备
  */
object TerminalEquipment {
	def main(args: Array[String]): Unit = {
		/**
		  * 创建sparksession对象
		  */
		val conf: SparkConf = new SparkConf()
		conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
		conf.registerKryoClasses(Array(classOf[Logs]))
		val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

	}

}
