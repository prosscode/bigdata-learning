package sparkStreaming.input

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: shawn pross
  * Date: 2018/05/18
  * Description:  监控102集群的端口，进行wordcount统计，并输出
  * 技术点：socketTextStream、
  */
object NetWordCount {
	def main(args: Array[String]): Unit = {
		/**
		  * 初始化程序入口
		  *
		  */
		val conf = new SparkConf().setMaster("local[2]").setAppName(s"${this.getClass.getSimpleName}")
		val sc = new SparkContext(conf)
		val ssc: StreamingContext = new StreamingContext(sc,Seconds(2))

		/**
		  *  通过程序入口获取DStream，
		  *  监控9999端口，并获取其内容，类型为ReceiverInputDStream[String                                         ]
		  */
		val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.123.102",9999)

		/**
		  * 对DStream流进行操作
		  */
		val wordCountDStream = dstream.flatMap(line => {
			line.split(",")
		}).map((_, 1)).reduceByKey(_ + _)

		wordCountDStream.print()

		ssc.start()
		ssc.awaitTermination()
		ssc.stop()
	}

}
