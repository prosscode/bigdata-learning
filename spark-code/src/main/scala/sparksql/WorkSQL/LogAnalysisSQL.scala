package sparksql.WorkSQL

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import sparksql.WorkSQL.ApacheLog.parseLog

/**
  * Author: shawn pross
  * Date: 2018/05/03
  * Description: 
  */
object LogAnalysisSQL {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setMaster("local").setAppName("LogAnalysisSQL")
		val sc = new SparkContext(conf)

		val sqlContext = new SQLContext(sc)
		//第一步，读取文件 RDD=>DF
		import sqlContext.implicits._
//		val df: Any = sc.textFile("E:\\BigData\\14_spark\\day08SQL\\资料\\log.txt")
//				.map(line => ApacheLog.parseLog(line)).toDF()
//		df.createOrReplaceGlobalTempView("log")

//		df.createOrReplaceTempView("log")
//		val df: DataFrame = sqlContext.read.format("text").load("E:\\BigData\\14_spark\\day08SQL\\资料\\log.txt")
//				.map(line=>ApacheLog.parseLog(line))
//		df.createOrReplaceGlobalTempView("log")

		val sql=
			"""
			  select
			  	min(contextSize),min(contextSize),avg(contentSize)
			  from log

			"""
		sqlContext.sql(sql).show()

		val sql2=
			"""
	  			select
	  				resposeCode,count(*)
	  			from log
	  				group by resposeCode
		""""
		val sql3= "select ipAddress,count(*) as count from log " +
				"			group by ipAddress having count > 2"


	}

}
