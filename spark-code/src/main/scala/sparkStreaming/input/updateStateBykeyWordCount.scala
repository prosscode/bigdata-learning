package sparkStreaming.input

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: shawn pross
  * Date: 2018/05/18
  * Description: updateStateByKey的使用
  *
  */
object updateStateBykeyWordCount {
	def main(args: Array[String]): Unit = {
		/**
		  * 初始化程序入口
		  */
		val conf = new SparkConf().setMaster("local[2]").setAppName(s"${this.getClass.getSimpleName}")
		val sc = new SparkContext(conf)
		val ssc: StreamingContext = new StreamingContext(sc,Seconds(2))

		/**
		  * 数据的输入
		  */
		val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.123.102",9999)

		/**
		  * 数据的处理
		  * Option:
		  * 	Some: 有值
		  * 	None: 没有值
		  * 	uodateFunc:(Seq[V],Option[S])=>Option[S]
		  *	数据的输入：
		  *		you，1
		  *		you，1
		  *		jump，1
		  *	bykey分组：
		  *		you,{1,1}
		  *		jump,{1}
		  *	value:Seq[Int]	List{1,1}
		  *	state:Option[Int] 上一次这个单词出现了多少次 None Some 2
		  *
		  * Example：
		  *	var f=(values:Seq[Int],state:Option[Int])=>{
		  *		val currentCount = values.sum
		  *		val lastCount = state.getOrElse(0)
		  *		Some(currentCount+lastCount)
		  *	}.updateStateByKey(f)
		  *
		  */
		val wordCountDStream: DStream[(String, Int)] = dstream.flatMap(_.split(",")).map((_, 1))
				.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
			val currentCount: Int = values.sum
			val lastCount = state.getOrElse(0)
			Some(currentCount + lastCount)
		})
		/**
		  * 数据的输出
		  */

		wordCountDStream.print()

		ssc.start()
		ssc.awaitTermination()
		ssc.stop()

	}

}
