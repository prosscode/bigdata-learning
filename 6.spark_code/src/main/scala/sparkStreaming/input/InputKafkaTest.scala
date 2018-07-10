package sparkStreaming.input

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: shawn pross
  * Date: 2018/05/19
  * Description: 
  */
object InputKafkaTest {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setMaster("local[2]").setAppName(s"${this.getClass.getSimpleName}")
		val sc = new SparkContext(conf)
		val ssc = new StreamingContext(sc,Seconds(2))
		ssc.checkpoint("hdfs://192.168.123.102:9000/streamingkafka")

		/**
		  * 数据的输入
		  */
		val kafkaParams = Map("metadata.broker.list"->"192.168.123.102:9092")
		val topics = Set("aura")
		val kafkaDSteream = KafkaUtils.createDirectStream[String,String,StringDecoder,StirngDecoder](ssc,kafkaParams,topics).map(_._2)

		/**
		  * 数据处理
		  * 比较正式的处理来源的数据
		  */
		kafkaDSteream.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_).print()

		ssc.start()
		ssc.awaitTermination()
		ssc.stop()
	}

}
