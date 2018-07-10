package sparkStreaming.input

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Author: shawn pross
  * Date: 2018/05/18
  * Description:  从HDFS上读取文件数据，wordcount统计并输出
  */
object HDFSWordCount {
	def main(args: Array[String]): Unit = {
		/**
		  * 初始化程序入口
		  */
		val conf = new SparkConf().setMaster("local[2]").setAppName(s"${this.getClass.getSimpleName}")
		val sc = new SparkContext(conf)
		val ssc = new StreamingContext(sc,Seconds(2))

		/**
		  * 数据输入
		  */
		val fileDStream: DStream[String] = ssc.textFileStream("hdfs://192.168.123.103:9000/streaming")

		/**
		  *  数据处理
		  */
		val wordCountDStream: DStream[(String, Int)] = fileDStream.flatMap(line => {
			line.split(",")
		}).map((_, 1)).reduceByKey(_ + _)
		wordCountDStream.print()

		ssc.start()
		ssc.awaitTermination()
		ssc.stop()
	}

}
