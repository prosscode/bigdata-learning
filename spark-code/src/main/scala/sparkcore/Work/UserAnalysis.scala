package sparkcore.Work

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Author: shawn pross
  * Date: 2018/05/02
  * Description:
  *
  *	数据格式：
  *	日期、用户id、会话id、页面id、访问时间、访问用户所在的城市、搜索关键字
  *	用户点击的品类id、用户点击的商品id、用户下单的品类id、用户下单的商品id、支付的品类id、支付的商品id、
  * 2018-03-11,user123,XXXXXYYYYY,1,1520769809971,1,"麻辣小龙虾|火锅鱼",
  * 	1,1,1^A2,1^A2,1^A2,1^A2
  * 2018-03-11,user1234,XX55YYYYY,1,1520769809972,1,"小龙虾|火锅鱼",
  * 	1,1,1^A2^A3,1^A2^A3,1^A2^A3,1^A2^A3
  * 2018-03-11,user1234,XX55YYYYY,1,1520769809973,1,"小龙虾|火锅鱼",
  * 	2,1,1^A2^A3,1^A2^A3,1^A3,1^A2^A3
  * 2018-03-11,user1234,XX55YYYYY,1,1520769809974,1,"小龙虾|火锅鱼",
  * 	2,1,1^A2^A3,1^A2^A3,1^A2^A3,1^A2^A3
  * 2018-03-11,user1234,XX55YYYYY,1,1520769809975,1,"小龙虾|火锅鱼",
  * 	4,1,1^A2^A3,1^A2^A3,1^A2^A3^A4,1^A2^A3
  *
  * 得出阶段性数据表
  * 表字段为：
  * 品类、点击次数、下单次数、支付次数
  *
  * 结果
  * 获取某一天获取点击下单支付次数排名前十的品类
  *
  *
  * 分析：
  * 1.获取数据里面涉及到的所有的品类ID
  * 	a.先获取所有的点击的品类id
  * 	b.获取所有下单的品类id
  * 	c.获取所有支付的品类id
  * 2.分别计算出来每个品类的点击次数，下单次数，支付次数
  * 3.所有品类id left join 每个品类的点击次数、下单次数、支付次数，即可得出阶段性数据表
  * 4.二次排序
  */
object UserAnalysis {

	private val conf: SparkConf = new SparkConf().setMaster("local").setAppName("UserAnalysis")
	private val sc = new SparkContext(conf)

	def main(args: Array[String]): Unit = {
		val rdd: RDD[String] = sc.textFile("E:\\BigData\\14_spark\\day07\\userAction.txt")
		//NO:1
		val allCategoryId: RDD[(Long, Long)] = getAllCategoryId(rdd).distinct()
		//NO:2 get_xxx_Category_id
		val click: RDD[(Long, Long)] = getClickCategoryCount(rdd)
		val order: RDD[(Long, Long)] = getOrderCategoryCount(rdd)
		val pay: RDD[(Long, Long)] = getPayCategoryCount(rdd)
		//NO:3 join
		val finalJoinResult: RDD[(Long, String)] = leftJoinRDD(allCategoryId,click,order,pay)

		/**
		  * Test result:
		  * 4	category_id=4 | click_category_count=1 | order_category_count=0 | pay_category_count=None
		  * 1	category_id=1 | click_category_count=2 | order_category_count=5 | pay_category_count=None
		  * 3	category_id=3 | click_category_count=0 | order_category_count=4 | pay_category_count=None
		  * 2	category_id=2 | click_category_count=2 | order_category_count=5 | pay_category_count=None
		  */
		finalJoinResult.foreach(tuple=>{
			println(tuple._1+"\t"+tuple._2)
		})

		//NO:4 sort
		sortByOrder(finalJoinResult)

		sc.stop()
	}

	/**
	  * get all category id in set
	  * @return
	  */
	def getAllCategoryId(rdd:RDD[String]): RDD[(Long,Long)] ={
		val set = new mutable.HashSet[(Long,Long)]
		rdd.flatMap(line=>{
			val fields = line.split(s"${Constant.SPLIT_CHAR}")
			val click_category_id = fields(7)
//			println(click_category_id)
			val order_category_id = fields(9)
			val pay_category_id = fields(11)

			if(click_category_id!=null && !click_category_id.trim.equals("")){
				set+=((click_category_id.toLong,click_category_id.toLong))
			}
			if(order_category_id!=null && !order_category_id.trim.equals("")){
				val fields = order_category_id.split(s"${Constant.CATEGORY_ID_SPLIT}")
				for(categoryid <- fields){
					set+=((categoryid.toLong,categoryid.toLong))
				}
			}
			if(pay_category_id!=null && !pay_category_id.trim.equals("")){
				val fields = pay_category_id.split(s"${Constant.CATEGORY_ID_SPLIT}")
				for(categoryid <- fields){
					set+=((categoryid.toLong,categoryid.toLong))
				}
			}
			set
		})
	}

	/**
	  * get Click Category Count
	  * @param rdd
	  * @return
	  */
	def getClickCategoryCount(rdd:RDD[String]): RDD[(Long,Long)] ={
		val clickRDD: RDD[(Long, Long)] = rdd.filter(line => {
			val fields = line.split(s"${Constant.SPLIT_CHAR}")
			fields(7) != null && !fields(7).trim.equals("")
		}).map(line => {
			val click_category_id = line.split(s"${Constant.SPLIT_CHAR}")(7).toLong
			(click_category_id, 1L)
		}).reduceByKey(_ + _)
		clickRDD
	}

	/**
	  * get order category Count
	  * @param rdd
	  * @return
	  */
	def getOrderCategoryCount(rdd:RDD[String]): RDD[(Long,Long)] ={
		val orderRDD = rdd.filter(line => {
			val fields = line.split(s"${Constant.SPLIT_CHAR}")(9)
			fields != null && !fields.trim.equals("")
		}).flatMap(line => {
			line.split(s"${Constant.SPLIT_CHAR}")(9).split(s"${Constant.CATEGORY_ID_SPLIT}")
		}).map(order_category_id => {
			(order_category_id.toLong, 1L)
		}).reduceByKey(_ + _)
		orderRDD
	}

	/**
	  * get pay category Count
	  * @param rdd
	  * @return
	  */
	def getPayCategoryCount(rdd:RDD[String]): RDD[(Long,Long)] ={
		val payRDD = rdd.filter(line => {
			val fields = line.split(s"${Constant.SPLIT_CHAR}")(11)
			fields != null && fields.trim.equals("")
		}).flatMap(line => {
			line.split(s"${Constant.SPLIT_CHAR}")(11).split(s"${Constant.CATEGORY_ID_SPLIT}")
		}).map(pay_category_id => {
			(pay_category_id.toLong, 1L)
		}).reduceByKey(_ + _)
		payRDD
	}


	def leftJoinRDD(all:RDD[(Long,Long)],click:RDD[(Long,Long)],
					order:RDD[(Long,Long)], pay:RDD[(Long,Long)]): RDD[(Long, String)] ={
		/**
		  * Option :if exist return some else none=0
		  */
		val all2click: RDD[(Long, (Long, Option[Long]))] = all.leftOuterJoin(click)
		val all2click2order = all2click.map(tuple => {
			val category_id = tuple._1.toLong
			val click_category_count = tuple._2._2.getOrElse(0)
			val value = s"${Constant.CATEGORY_ID}=" + category_id +
					s" | ${Constant.CLICK_CATEGORY_COUNT}=" + click_category_count
			(category_id, value)
		}).leftOuterJoin(order)

		val all2click2order2pay = all2click2order.map(tuple => {
			val category_id = tuple._1.toLong
			var value = tuple._2._1
			val order_category_count = tuple._2._2.getOrElse(0)
			value += s" | ${Constant.ORDER_CATEGORY_COUNT}=" + order_category_count
			(category_id, value)
		}).leftOuterJoin(pay)

		val finalLeftJoinRedult: RDD[(Long, String)] = all2click2order2pay.map(tuple => {
			val category_id = tuple._1.toLong
			var value = tuple._2._1
			val pay_category_count = tuple._2._2
			value += s" | ${Constant.PAY_CATEGORY_COUNT}=" + pay_category_count
			(category_id, value)
		})
		finalLeftJoinRedult
	}


	/**
	  * by order_category_count sorted of desc
	  * @param rdd
	  */
	def sortByOrder(rdd:RDD[(Long,String)]): Unit ={
		val finalSortedResult: RDD[(Long, Long)] = rdd.map(line => {
			val order_count: Long = line._2.split(s"${Constant.SORT_SPLIT_CHAR_1}")(2).split(s"${Constant.SORT_SPLIT_CHAR_2}")(1).trim.toLong
			val category_id = line._1
			(category_id, order_count)
		}).sortBy(_._2, false)
		finalSortedResult.foreach(result=>{
			println(s"${Constant.CATEGORY_ID}="+result._1)
		})
	}
}
