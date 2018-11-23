package sparkStreaming.output

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import sparkStreaming.ConnectionPool

/**
  * Author: shawn pross
  * Date: 2018/05/19
  * Description:  数据的输出，把结果保存到mysql中
  */
object OutoutTest {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
		val sc = new  SparkContext(conf)
		val ssc = new StreamingContext(sc,Seconds(2))
		//设置checkpoint的目录
		ssc.checkpoint("hdfs://192.168.123.102:9000/streamingcheckpoint3")

		/**
		  * 数据的输入
		  */
		val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.123.102",8888)

		/**
		  * 数据计算
		  */
		val wordCountDStream: DStream[(String, Int)] = dstream.flatMap(_.split(",")).map((_, 1)).updateStateByKey((values: Seq[Int], state: Option[Int]) => {
			val currentCount = values.sum
			val lastCount = state.getOrElse(0)
			Some(currentCount + lastCount)
		})
		/**
		  * 数据的输出
		  */
		wordCountDStream.foreachRDD(rdd=>{
			rdd.foreachPartition(partition=> {
				val conn = ConnectionPool.getConnection()
				val statement = conn.createStatement()
				partition.foreach {
					case (word, count) => {
						val sql = s"insert into tableName values(now(),'$word','$count')"
						print(sql)
						statement.execute(sql)
					}
				}
				ConnectionPool.returnConnection(conn)
			})
		})

		ssc.start()
		ssc.awaitTermination()
		ssc.stop()

	}

}
