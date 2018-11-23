package sparkcore.TransfOP

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: shawn pross
  * Date: 2018/04/23
  * Description:  use scala programming to achivement transformations
  */
class OP(){
	val conf = new SparkConf().setAppName("transformation").setMaster("local")
	val sc = new SparkContext(conf)

	/**
	  * flatMap usage
	  */
	def flatMap(): Unit ={
		val array = Array("1,pross","2,kris","3,benny")
		val arrayRDD = sc.parallelize(array)
		arrayRDD.flatMap(_.split(",")).foreach(x=>print(x+","))
	}

	/**
	  * map usage
	  */
	def map(): Unit ={
		val array = Array("pross","kris","benny")
		val rdd = sc.parallelize(array)
		rdd.map(word=>"hello "+word+",").foreach(x=>print(x))
	}

	/**
	  * filter usage
	  */
	def filter(): Unit ={
		val array = Array(1,2,3,4,5,6,7,8,9,10)
		val RDD = sc.parallelize(array)
		RDD.filter(num=>num%2==0).foreach(x=>print(x+","))
	}

	/**
	  * groupByKey usage
	  */
	def groupByKey(): Unit ={
		val array = Array(("hello", "word"), ("hello", "scala"), ("hi", "word"), ("hi", "scala"))
		val RDD = sc.parallelize(array)
		val unit = RDD.groupByKey()
		unit.map(word=>
			(word._1,word._2.mkString("-"))
		).foreach(x=>print(x))
	}

	/**
	  * reduceBykey usage
	  */
	def reduceByKey(): Unit ={
		val array = Array(("hello", "word"), ("hello", "scala"), ("hi", "word"), ("hi", "scala"))
		val rdd = sc.parallelize(array)
		rdd.reduceByKey((a,b)=>(a+"-"+b)).collect().foreach(x=>print(x))
	}

	/**
	  *sortByKey usage
	  */
	def sortByKey(): Unit ={
		val array = Array((1,"pross"),(5,"kris"),(10,"benny"),(2,"error"))
		val RDD = sc.parallelize(array)
		RDD.sortByKey(false).foreach(x=>print(x))
	}


	/**
	  * join usage
	  * 相当于SQL中的内关联join,只返回两个RDD根据K可以关联上的结果
	  */
	def join(): Unit ={
		val name = Array((1,"pross"),(5,"kris"),(10,"benny"),(2,"error"),(3,"zks"))
		val score = Array((1,90),(10,95),(2,80),(10,85))
		val nameRDD = sc.parallelize(name)
		val scoreRDD = sc.parallelize(score)
		nameRDD.join(scoreRDD).sortByKey(true)
				.foreach(x=>print("("+x._1+","+x._2._1+","+x._2._2+")"))
	}

	/**
	  * cogroup usage
	  * 可以指定Partitions分区数和分区函数
	  * cogroup相当于SQL中的全外关联full outer join，返回左右RDD中的记录，关联不上的为空。
	  */
	def cogroup(): Unit ={
		val name = Array((1,"pross"),(5,"kris"),(10,"benny"),(2,"error"),(3,"zks"))
		val RDD1= sc.parallelize(name)
		val score = Array((1,90),(10,95),(2,80),(5,85))
		val RDD2 = sc.parallelize(score)

		RDD1.cogroup(RDD2).sortByKey(true).foreach(tuple=>{
			val id=tuple._1
			val name=tuple._2._1.mkString("")
			val score=tuple._2._2.mkString("")
			println(id+"-"+name+"-"+score)
		})

	}
	/**
	  * union usage
	  * no distinct
	  */
	def union(): Unit ={
		val add = Array(1,3,5,7,9,2)
		val even = Array(2,4,6,8,10,1)
		val addRDD = sc.parallelize(add)
		val evenRDD = sc.parallelize(even)
		addRDD.union(evenRDD).collect().foreach(x=>print(x+","))
	}

	/**
	  * union usage
	  * & distinct
	  */
	def unionDistinct(): Unit ={
		val RDD1 = sc.parallelize(Array(1,2,3,4,5))
		val RDD2 = sc.parallelize(Array(5,6,7,8,9))
		RDD1.union(RDD2).distinct().
				collect().foreach(x=>print(x))
	}

	/**
	  * intersection usage
	  * 求交集
	  */
	def intersection(): Unit ={
		val RDD1 = sc.parallelize(Array(1,2,3,4,5))
		val RDD2 = sc.parallelize(Array(5,6,7,8,9))
		RDD1.intersection(RDD2).foreach(x=>print(x))

	}

	/**
	  * cartesian usage
	  * 笛卡尔积
	  */
	def cartesian(): Unit ={
		val RDD1 = sc.parallelize(Array("a","b","c"))
		val RDD2 = sc.parallelize(Array(5,6,7))
		RDD1.cartesian(RDD2).foreach(x=>print(x))
	}

	/**
	  * mapPartitions usage
	  * map升级版，每次读取一个分区的内容
	  */
	def mapPartitions(): Unit ={
		val RDD = sc.parallelize((Array("hello","world","pross","kris","error","benny")),2)
		RDD.mapPartitions{num=>{
			val result=List[String]()
			var i="hello "
			while(num.hasNext){
				i+=num.next()
			}
			//往集合result的头部追加元素i，并转化为迭代器返回出去
			result.::(i).iterator
		}}.foreach(x=>print(x))
	}

	/**
	  * mapPartitionsWithIndex usage
	  * 每次读取一个分区的内容并可以在参数中加入分区的索引
	  */
	def mapPartitionsWithIndex(): Unit ={
		val RDD = sc.parallelize((Array("hello","world","pross","kris","error","benny")),2)
		// x是分区号，collect是分区内容
		RDD.mapPartitionsWithIndex{(x,collect)=>{
			val result=List[String]()
			var i="hello "
			while(collect.hasNext){
				i+=collect.next()
			}
			result.::(x+"_"+i).iterator
		}}.foreach(x=>print(x))
	}

	/**
	  * coalesce usage
	  * 用于将RDD进行重分区，第一个参数为重分区的数据，第二个是是否进行shuffle，默认为false
	  * 如果重分区的数目大于原来的分区数，那么必须指定shuffle参数为true，否则，分区数不变
	  */
	def coalesce(): Unit ={
		val array = Array(1,2,3,4,5,6,7,8,9)
		val RDD = sc.parallelize(array,2)
		RDD.coalesce(3,true).foreach(x=>print(x))
	}
	/**
	  * repartition usage
	  * 进行重分区，实际上就是coalesce函数的第二个参数为true的实现
	  */
	def repartition(): Unit ={
		val RDD = sc.parallelize((Array("hello","world","pross","kris","error","benny")),3)
		RDD.repartition(2).foreach(x=>print(x))
	}

	/**
	  * sample usage
	  * 第一个参数withReplacement：Boolean ，true 有放回抽样  false 无放回抽样
	  * 第二个参数fraction：Double，概率
	  * 第三个参数seed：Long，随机种子
 	  */
	def sample(): Unit ={
		val array = Array(1,2,3,4,5,6,7,8,9,10)
		val RDD = sc.parallelize(array)
		RDD.sample(false,0.5).foreach(x=>println(x))
	}

	/**
	  * pipe usage
	  * 管道命令，执行外部程序或脚本
	  */
	def pipe(): Unit ={
		val array = Array(1,2,3,4,5,6,7,8,9,10)
		val RDD = sc.parallelize(array)
		RDD.pipe("Path")
	}
}

object TransformationsScalaOP {
	def main(args: Array[String]): Unit = {
		val op = new OP
//		op.flatMap()
//		op.map()
//		op.filter()
//		op.groupByKey()
//		op.reduceByKey()
//		op.sortByKey()
//		op.join()
//		op.cogroup()
//		op.union()
//		op.unionDistinct()
//		op.intersection()
//		op.cartesian()
//		op.mapPartitions()
//		op.mapPartitionsWithIndex()
//		op.coalesce()
//		op.repartition()
		op.sample()
	}
}
