package sparkcore.Work

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: shawn pross
  * Date: 2018/05/02
  * Description:
  * 需求一：
  * The average, min, and max content size of responses returned from the server.
  *
  * select avg/min/max(xxx) from log
  *
  * 需求二：
  * A count of response code's returned.
  *
  *
  *
  * 需求三：
  * All IPAddresses that have accessed this server more than 100 times.
  * 哪些IP地址访问我们的网站超过N次
  *
  * select IPAddresses,count(IPAddresses) as count as from log group by IPAddresses
  * 		having count>N
  *
  * 需求四：
  * The top endpoints requested by count.  TopN
  * 找出被访问次数最多的地址的前三个
  *
  * select endpoints,count(endpoints) as count from log group by endpoints
  * 		order by count desc limit 3
  *
  */
object LogAnalysis {
	val conf = new SparkConf().setAppName("LogAnalysis").setMaster("local")
	val sc = new SparkContext(conf)
	val RDD = sc.textFile("log.txt")

	def MinMaxAvg(): Unit ={

		RDD.map(line=>line.split("\\#")(6).toInt).max()

		RDD.map(line=>(line.split("\\#")(5),1)).reduceByKey(_+_)

		RDD.map(line=>(line.split("\\#")(0),1)).reduceByKey(_+_).filter(tuple=>{
			tuple._2>2
		})

		RDD.map(line=>((line.split("\\#")(4)).split(" ")(1),1)).reduceByKey(_+_)
				.sortBy(_._2,false).take(2)


		sc.stop()

	}

	def main(args: Array[String]): Unit = {
		MinMaxAvg()
	}
}
