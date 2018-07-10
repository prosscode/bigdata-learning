package sparkStreaming.input

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: shawn pross
  * Date: 2018/05/18
  * Description:	Driver服务
  * 	利用checkpoint恢复Driver服务
  * 	继续从上一次的wordcount的数据开始记录
  * 技术点：	socketTextStream、updateStateByKey、
  * StreamingContext.getOrCreate
  */
object DriverHAWordCount {
	def main(args: Array[String]): Unit = {
		val checkpointDirectory:String="hdfs://192.168.123.102:9000/streamingcheckpoint";

		def functionToCreateContext(): StreamingContext = {
			/**
			  * 正常的wordcount代码实现
			  */
			val conf = new SparkConf().setMaster("local[2]").setAppName("NetWordCount")
			val sc = new SparkContext(conf)
			val ssc = new StreamingContext(sc,Seconds(2))
			ssc.checkpoint(checkpointDirectory)
			val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.123.102",9999)
			val wordCountDStream = dstream.flatMap(_.split(","))
					.map((_, 1))
					.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
						val currentCount = values.sum
						val lastCount = state.getOrElse(0)
						Some(currentCount + lastCount)
					})
			wordCountDStream.print()
			ssc.start()
			ssc.awaitTermination()
			ssc.stop()
			//把结果返回出去
			ssc
		}

		/**
		  * 从检查点数据获取StreamingContext,没有就创建一个新的
		  * 参数：checkpoint目录、wordcount函数实现
		  */
		val ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)
	}

}
