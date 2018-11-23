package sparksql.UDAF

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Author: shawn pross
  * Date: 2018/05/03
  * Description: 
  */
object UDAFTest {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		val sc = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)
//		val value = sqlContext.udf.register("salary_avg",UDAFDemo)

		sqlContext.sql("select salary_avg(salary) from log").show()

	}

}
