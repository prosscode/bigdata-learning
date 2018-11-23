package graphx

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{GraphLoader, VertexRDD}

/**
  * Author: shawn pross
  * Date: 2018/05/17
  * Description: spark graphx实现共同好友的聚合
  * 数据源格式：
  * 	1，2
  * 	2，4
  */
object GraphRale {
	/**
	  * 数据列表的笛卡尔积：｛1，2，3，4｝=> {(1,2),(1,3),(1,4),(2,3),...}
	  * @param input
	  * @return
	  */
	def ciculate(input:List[Long]): Set[String] ={
		var result=Set[String]()
		input.foreach(x=>{
			input.foreach(y=>{
				if(x<y){
					result += s"${x} | ${y}"
				}else if(x>y){
					result += s"${y} | ${x}"
				}
			})
		})
		return result
	}

	/**
	  * 聚合操作
	  */
	def twoDegress(): Unit ={
		val conf = new SparkConf().setMaster("local").setAppName("graph")
		val sc = new SparkContext(conf)
		val graph = GraphLoader.edgeListFile(sc,"E:\\BigData\\14_spark\\day15_graphx\\users.txt")
		val relate: VertexRDD[List[Long]] = graph.aggregateMessages[List[Long]](
			triplet=>{
				triplet.sendToDst(List(triplet.srcId))
			},
			(a,b)=>(a++b)
		).filter(x=>x._2.length>1)

		val re = relate.flatMap(x=>{
			for{temp <- ciculate(x._2)}yield (temp,1)
		}).reduceByKey(_+_)

		re.foreach(println(_))
	}

	def main(args: Array[String]): Unit = {
		twoDegress()
	}
}
