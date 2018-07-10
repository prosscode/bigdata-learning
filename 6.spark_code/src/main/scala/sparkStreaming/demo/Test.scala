package sparkStreaming.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import sparkStreaming.ConnectionPool

/**
  * Created by Administrator on 2018/5/10.
  */
object Test {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("NetworkWordCountForeachRDD")
    sparkConf.setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(sc, Seconds(3))

    //创建一个接收器(ReceiverInputDStream)，这个接收器接收一台机器上的某个端口通过socket发送过来的数据并处理
    val lines = ssc.socketTextStream("hadoop1", 9999)

    lines.print(3)

    //处理的逻辑，就是简单的进行word count
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    //将结果保存到Mysql(七)
    wordCounts.foreachRDD { (rdd, time) =>
      rdd.foreachPartition { partitionRecords =>
        val conn = ConnectionPool.getConnection
        conn.setAutoCommit(false)
        val statement = conn.prepareStatement(s"insert into 1711test(ts, word, count) values (?, ?, ?)")
        partitionRecords.zipWithIndex.foreach { case ((word, count), index) =>
          statement.setLong(1, time.milliseconds)
          statement.setString(2, word)
          statement.setInt(3, count)
          statement.addBatch()
          if (index != 0 && index % 500 == 0) {
            statement.executeBatch()
            conn.commit()
          }
        }
        statement.executeBatch()
        statement.close()
        conn.commit()
        conn.setAutoCommit(true)
        ConnectionPool.returnConnection(conn)
      }
    }

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

}
