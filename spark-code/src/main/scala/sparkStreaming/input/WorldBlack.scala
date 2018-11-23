package sparkStreaming.input

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: shawn pross
  * Date: 2018/05/18
  * Description:  黑名单进行过滤
  *	技术点：socketTextStream、transform、leftOuterJoin
  */
object WorldBlack {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setMaster("local[2]").setAppName(s"${this.getClass.getSimpleName}")
		val sc = new SparkContext(conf)
		val ssc = new StreamingContext(sc,Seconds(2))

		/**
		  * 数据的输入
		  * 模拟一个黑名单（正常情况下， 黑名单是从mysql、redis、HBase中读取出来的）
		  */
		val wordBlackList: Array[(String, Boolean)] = ssc.sparkContext.parallelize(List("?", "*"))
				.map(param => (param, true)).collect()
		val blackListBroadcast: Broadcast[Array[(String, Boolean)]] = ssc.sparkContext.broadcast(wordBlackList)
		val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.123.102",9999)

		/**
		  * 数据的处理
		  */
		val wordOneDStream: DStream[(String, Int)] = dstream.flatMap(_.split(",")).map((_,1))
		//transform 需要有返回值，必须类型是RDD
		val wordCountDStream = wordOneDStream.transform(rdd => {
			val filterRDD = rdd.sparkContext.parallelize(blackListBroadcast.value)
			val resultRDD = rdd.leftOuterJoin(filterRDD)
			resultRDD.filter(tuple => {
				tuple._2._2.isEmpty
			}).map(_._1)
		}).map((_, 1)).reduceByKey(_ + _)


		/**
		  * 数据的输出
		  */
		wordCountDStream.print()

		ssc.start()
		ssc.awaitTermination()
		ssc.stop()
	}

}
