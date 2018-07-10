package sparksql.SQL2DB

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Author: shawn pross
  * Date: 2018/05/03
  * Description:  连接数据库,读取mysql中的数据
  */
object MySQLTest {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setMaster("local").setAppName("LogAnalysisSQL")
		val sc = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)

		val  url="jdbc:mysql://localhost:3306/test"
		val table_name="score"
		val properties = new Properties()
		properties.put("user","root")
		properties.put("password","root")
		val df: DataFrame = sqlContext.read.jdbc(url,table_name,properties)

		df.createOrReplaceGlobalTempView("score")
		sqlContext.sql("select * from score").show()

	}

}
