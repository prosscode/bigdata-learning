package sparkStreaming.demo.flume

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

/**
  * Produces a count of events received from Flume.
  *
  * This should be used in conjunction with the Spark Sink running in a Flume agent. See
  * the Spark Streaming programming guide for more details.
  *
  * Pull-based Approach using a Custom Sink(Spark Streaming作为一个Sink存在)
  *
  * 1、将jar包scala-library_2.11.8.jar、commons-lang3-3.5.jar、spark-streaming-flume-sink_2.11-2.2.0.jar
  * 放置在master上的/usr/local/soft/apache-flume-1.8.0-bin/lib下
  *
  * 2、配置usr/local/soft/spark-streaming-flume/apache-flume-1.8.0-bin/conf/flume-conf.properties
  *
  * 3、启动flume的agent
  * bin/flume-ng agent -n agent1 -c conf -f conf/flume-conf.properties
  *
  * 4、启动Spark Streaming应用
  * spark-submit --class com.aura.streaming.flume.FlumeEventCountPullBased \
  * --master spark://master:7077 \
  * --deploy-mode client \
  * --driver-memory 512m \
  * --executor-memory 512m \
  * --total-executor-cores 4 \
  * --executor-cores 2 \
  *
  * usr/local/soft/streaming/spark-streaming-datasource-1.0-SNAPSHOT-jar-with-dependencies.jar \
  * master 44446
  * *
  * 3、在master上 telnet master 44445 发送消息
  * *
  * http://spark.apache.org/docs/1.6.0/streaming-flume-integration.html
  * *
  * http://spark.apache.org/docs/2.3.0/streaming-flume-integration.html
  *
  */
object FlumeEventCountPullBased {
	def main(args: Array[String]) {
		if (args.length < 2) {
			System.err.println(
				"Usage: FlumePollingEventCount <host> <port>")
			System.exit(1)
		}

		val Array(host, port) = args

		val batchInterval = Milliseconds(2000)

		// Create the context and set the batch size
		val sparkConf = new SparkConf().setAppName("FlumePollingEventCount")
		val ssc = new StreamingContext(sparkConf, batchInterval)

		// Create a flume stream that polls the Spark Sink running in a Flume agent
		val stream = FlumeUtils.createPollingStream(ssc, host, port.toInt)

		// Print out the count of events received from this server in each batch
		stream.count().map(cnt => "Received " + cnt + " flume events.").print()

		ssc.start()
		ssc.awaitTermination()
	}
}
