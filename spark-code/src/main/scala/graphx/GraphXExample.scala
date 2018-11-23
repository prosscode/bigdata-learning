package graphx

/**
  * Author: shawn pross
  * Date: 2018/05/16
  * Description: 
  */
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphXExample {
	def main(args: Array[String]): Unit = {
		/**
		  * 初始化
		  */

		val conf = new SparkConf().setAppName("GraphXExample").setMaster("local")
		val sc = new SparkContext(conf)

		/**
		  * 设置顶点和边，注意顶点和边都是用元组定义的Array
		  *
		  * 顶点的数据类型是VD:(String,Int)
		  * 边的数据类型是ED:Int
		  */
		val verteArray: Array[(Long, (String, Int))] = Array(
			(1L, ("Alice", 28)),
			(2L, ("Bob", 24)),
			(3L, ("Pross", 23)),
			(4L, ("Kris", 29)),
			(5L, ("David", 20)),
			(6L, ("Fran", 18))
		)
		val edgeArray: Array[Edge[Int]] = Array(
			Edge(2L, 1L, 7),
			Edge(2L, 4L, 2),
			Edge(3L, 2L, 4),
			Edge(3L, 6L, 3),
			Edge(4L, 1L, 1),
			Edge(5L, 2L, 2),
			Edge(5L, 3L, 8),
			Edge(5L, 6L, 3)
		)
		/**
		  * 构造vertexRDD和edgeRDD
		  */
		val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(verteArray)
		val edgeRDD:RDD[Edge[Int]] = sc.parallelize(edgeArray)

		/**
		  * 构造图GraphX[VD,ED]
		  */
		val graph: Graph[(String, Int), Int] = Graph(vertexRDD,edgeRDD)

		/**
		  * 图的属性
		  */
		println("*****属性演示*****")
		print("找出图中年龄大于30的顶点：")
		graph.vertices.filter{
			case (id,(name,age))=> age > 30
		}.collect().foreach{
			case (id,(name,age)) => println(s"$name is $age")
		}

		//边操作：找出图中属性大于5的边
		print("找出图中属性大于5的边：")
		graph.edges.filter(e=>e.attr > 5).collect()
				.foreach(e=>println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))

		//triplets操作， ((srcId,srcAttr),(dstId,dstAttr),attr) 三元组
		println("列出边属性>5的tripltes：")
		for(triplet<-graph.triplets.filter(t=>t.attr>5).collect()){
			println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
		}

		//Degress操作
		println("找出图中最大的出度，入度，度数：")
		def max(a:(VertexId,Int),b:(VertexId,Int)): (VertexId,Int) ={
			if (a._2>b._2) a else b
		}

		println("max of outDegress："+graph.outDegrees.reduce(max)
				+"max of inDegress："+graph.inDegrees.reduce(max)
				+"max of Degress："+graph.degrees.reduce(max))

		/**
		  * 转换操作
		  */
		println("*****转换操作*****")
		println("顶点的转换操作，顶点age + 10：")
		graph.mapVertices{ case (id, (name, age)) => (id, (name, age+10))}.vertices.collect
				.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
		//边的转换
		println("边的转换操作，边的属性*2：")
		graph.mapEdges(e=>e.attr*2).edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))


		/**
		  * 结构操作
		  */
		println("*****结构操作*****")
		println("顶点年纪>30的子图：")
		val subGraph = graph.subgraph(vpred = (id, vd) => vd._2 >= 30)

		//子图所有的顶点
		println("子图所有顶点：")
		subGraph.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))

		//子图所有的边
		println("子图所有边：")
		subGraph.edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))


		/**
		  * 连接操作
		  */
		println("*****连接操作*****")
		val inDegrees: VertexRDD[Int] = graph.inDegrees
		case class User(name: String, age: Int, inDeg: Int, outDeg: Int)

		//创建一个新图，顶点VD的数据类型为User，并从graph做类型转换
		val initialUserGraph: Graph[User, Int] = graph.mapVertices {
			case (id, (name, age)) => User(name, age, 0, 0)
		}

		//initialUserGraph与inDegrees、outDegrees（RDD）进行连接，并修改initialUserGraph中inDeg值、outDeg值
		val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
			case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
		}.outerJoinVertices(initialUserGraph.outDegrees) {
			case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg,outDegOpt.getOrElse(0))
		}

		println("连接图的属性：")
		userGraph.vertices.collect.foreach(v => println(s"${v._2.name} inDeg: ${v._2.inDeg}  outDeg: ${v._2.outDeg}"))
		println("出度和入读相同的人员：")
		userGraph.vertices.filter {
			case (id, u) => u.inDeg == u.outDeg
		}.collect.foreach {
			case (id, property) => println(property.name)
		}

		/**
		  * 聚合操作
		  */
		println("*****聚合操作*****")
		println("找出5到各顶点的最短：")
		val sourceId: VertexId = 5L // 定义源点
		val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
		val sssp = initialGraph.pregel(Double.PositiveInfinity)(
			(id, dist, newDist) => math.min(dist, newDist),
			triplet => {  // 计算权重
				if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
					Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
				} else {
					Iterator.empty
				}
			},
			(a,b) => math.min(a,b) // 最短距离
		)
		println(sssp.vertices.collect.mkString("\n"))
		sc.stop()
	}

}
