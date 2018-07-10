package sparkStreaming.input

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: shawn pross
  * Date: 2018/05/19
  * Description:
  *  需求：实现一个 每隔4秒 统计最近6秒的单词计数的情况
  *  技术点：reduceByKeyAndWindow方法
  */
object WindowOperatorTest {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setMaster("local[2]").setAppName(s"${this.getClass.getSimpleName}")
		val sc = new SparkContext(conf)
		val ssc = new StreamingContext(sc,Seconds(2))

		/**
		  * 数据输入
		  */
		val dstream = ssc.socketTextStream("102.168.123.102",9999)

		/**
		  * 数据的处理
		  * reduceFunc:(V,V)=V
		  * windowDuration：Duration，6，窗口大小
		  * slideDuration：Duration，4，滑动大小
		  * numPartitions：Int，指定分区数
		  */
		val resultWordCountDStream: DStream[(String, Int)] = dstream.flatMap(_.split(",")).map((_, 1))
				.reduceByKeyAndWindow((x: Int, y: Int) =>
					x + y, Seconds(6), Seconds(4)
		)

		/**
		  *  数据输出
		  */
		resultWordCountDStream.print()

		ssc.start()
		ssc.awaitTermination()
		ssc.stop()
	}

}
