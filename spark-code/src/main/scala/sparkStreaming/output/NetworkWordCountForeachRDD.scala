package sparkStreaming.output

import java.sql.DriverManager

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import sparkStreaming.ConnectionPool

/**
  * WordCount程序，Spark Streaming消费TCP Server发过来的实时数据的例子：
  *
  * 1、在master服务器上启动一个Netcat server
  * `$ nc -lk 9998` (如果nc命令无效的话，我们可以用yum install -y nc来安装nc)
  *
  *
  * create table wordcount(ts bigint, word varchar(50), count int);
  *
  * spark-shell --total-executor-cores 4 --executor-cores 2 --master spark://master:7077 --jars mysql-connector-java-5.1.44-bin.jar,c3p0-0.9.1.2.jar,spark-streaming-basic-1.0-SNAPSHOT.jar
  *
  *
  */
object NetworkWordCountForeachRDD {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("NetworkWordCountForeachRDD")
    val sc = new SparkContext(sparkConf)

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(sc, Seconds(5))

    //创建一个接收器(ReceiverInputDStream)，这个接收器接收一台机器上的某个端口通过socket发送过来的数据并处理
    val lines = ssc.socketTextStream("hadoop1", 9998, StorageLevel.MEMORY_AND_DISK_SER)

    //处理的逻辑，就是简单的进行word count
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    //将结果保存到Mysql(一)
    /**
      *
      * 这个代码会报错的！！！
      */
    wordCounts.foreachRDD { (rdd, time) =>
      Class.forName("com.mysql.jdbc.Driver")
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/aura", "root", "root")
      val statement = conn.prepareStatement(s"insert into wordcount(ts, word, count) values (?, ?, ?)")
      rdd.foreach { record =>
        statement.setLong(1, time.milliseconds)
        statement.setString(2, record._1)
        statement.setInt(3, record._2)
        statement.execute()
      }
      statement.close()
      conn.close()
    }
    //启动Streaming处理流
    ssc.start()

    ssc.stop(false)


    //将结果保存到Mysql(二)
    wordCounts.foreachRDD { (rdd, time) =>
      rdd.foreach { record =>
        Class.forName("com.mysql.jdbc.Driver")
        val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/aura", "root", "root")
        val statement = conn.prepareStatement(s"insert into wordcount(ts, word, count) values (?, ?, ?)")
        statement.setLong(1, time.milliseconds)
        statement.setString(2, record._1)
        statement.setInt(3, record._2)
        statement.execute()
        statement.close()
        conn.close()
      }
    }

    //将结果保存到Mysql(三)
    wordCounts.foreachRDD { (rdd, time) =>
      rdd.foreachPartition { partitionRecords =>
        Class.forName("com.mysql.jdbc.Driver")
        val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/aura", "root", "root")
        val statement = conn.prepareStatement(s"insert into wordcount(ts, word, count) values (?, ?, ?)")
        partitionRecords.foreach { case (word, count) =>
          statement.setLong(1, time.milliseconds)
          statement.setString(2, word)
          statement.setInt(3, count)
          statement.execute()
        }
        statement.close()
        conn.close()
      }
    }

    //将结果保存到Mysql(四)
    wordCounts.foreachRDD { (rdd, time) =>
      rdd.foreachPartition { partitionRecords =>
        val conn = ConnectionPool.getConnection
        val statement = conn.prepareStatement(s"insert into wordcount(ts, word, count) values (?, ?, ?)")
        partitionRecords.foreach { case (word, count) =>
          statement.setLong(1, time.milliseconds)
          statement.setString(2, word)
          statement.setInt(3, count)
          statement.execute()
        }
        statement.close()
        ConnectionPool.returnConnection(conn)
      }
    }

    //将结果保存到Mysql(五)
    wordCounts.foreachRDD { (rdd, time) =>
      rdd.foreachPartition { partitionRecords =>
        val conn = ConnectionPool.getConnection
        val statement = conn.prepareStatement(s"insert into wordcount(ts, word, count) values (?, ?, ?)")
        partitionRecords.foreach { case (word, count) =>
          statement.setLong(1, time.milliseconds)
          statement.setString(2, word)
          statement.setInt(3, count)
          statement.addBatch()
        }
        statement.executeBatch()
        statement.close()
        ConnectionPool.returnConnection(conn)
      }
    }


    //将结果保存到Mysql(六)
    wordCounts.foreachRDD { (rdd, time) =>
      rdd.foreachPartition { partitionRecords =>
        val conn = ConnectionPool.getConnection
        conn.setAutoCommit(false)
        val statement = conn.prepareStatement(s"insert into wordcount(ts, word, count) values (?, ?, ?)")
        partitionRecords.foreach { case (word, count) =>
          statement.setLong(1, time.milliseconds)
          statement.setString(2, word)
          statement.setInt(3, count)
          statement.addBatch()
        }
        statement.executeBatch()
        statement.close()
        conn.commit()
        conn.setAutoCommit(true)
        ConnectionPool.returnConnection(conn)
      }
    }


    //将结果保存到Mysql(七)
    wordCounts.foreachRDD { (rdd, time) =>
      rdd.foreachPartition { partitionRecords =>
        val conn = ConnectionPool.getConnection
        conn.setAutoCommit(false)
        val statement = conn.prepareStatement(s"insert into wordcount(ts, word, count) values (?, ?, ?)")
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

    //等待Streaming程序终止
    ssc.awaitTermination()
  }
}
