package graphx

import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: shawn pross
  * Date: 2018/05/17
  * Description: 
  */
object DmpGraphX {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
		conf.setAppName(s"${this.getClass.getSimpleName}")
		conf.setMaster("local")
		val sc = new SparkContext(conf)

		/**
		  * person.txt 数据格式
		  *
		  * 2 1
		  * 4 1
		  * 3 2
		  * 4 3
		  * 3 4
		  */
		val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, "E:\\BigData\\14_spark\\day15_graphx\\person.txt")
		graph.vertices.foreach(print(_))

		/**
		  * result.txt 数据格式
		  *
		  * 1	pross	D00020005:2	D00010003:2	APP马上赚:2	ZC上海市:2	D00010001:2	LC00:2	ZP上海市:2
		  * 2	maxpross	D00020005:2	D00010003:2	APP马上赚:1	ZC上海市:2	D00010001:2	LC00:2	ZP上海市:2
		  * 3	kris	D00020005:1	D00010003:1	APP马上赚:2	ZC上海市:2	D00010001:2	LC00:2	ZP武汉市:3
		  * 4	shawn	D00020005:2	D00010003:2	APP马上赚:1	ZC上海市:2	D00010001:2	LC00:2	ZP上海市:2
		  */
		sc.textFile("E:\\BigData\\14_spark\\day15_graphx\\result.txt").map(line => {
			val fields: Array[String] = line.split("\t")
		})
	}

}
