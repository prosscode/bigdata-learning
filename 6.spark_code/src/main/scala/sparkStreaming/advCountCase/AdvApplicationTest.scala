package sparkStreaming.advCountCase

import java.sql.Date
import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import sparkStreaming.ConnectionPool
import sparkStreaming.demo.utils.DateUtils


/**
  * Created by Administrator on 2018/5/12.
  *
  * timestamp:
  * 时间戳，用户点击广告的时间
  * province:
  * 省份，用户在哪个省份点击的广告
  * city:
  * 城市，用户在哪个城市点击的广告
  * userid:
  * 用户的唯一标识
  * advid:
  * 被点击的广告id
  */
object AdvApplicationTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("AdvApplicationTest")
    conf.set("","")  //序列化

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc,Seconds(5))

    val spark = SparkSession.builder()
      .config(conf).getOrCreate()

    /**
      * 第一步：从kafka获取数据（direct  方式）
      *   K: ClassTag,
          V: ClassTag,
          KD <: Decoder[K]: ClassTag,
          VD <: Decoder[V]: ClassTag] (
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      topics: Set[String]
      */
    val kafkaParams = Map("metadata.broker.list" -> "hadoop1:9092")
    val topics = Set("aura")
    val logDstream: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics).map(_._2)

    /**
      * 第二步：进行黑名单过滤
      */
    val filterLogDStream: DStream[String] = blackListFilter(logDstream,ssc)


    /**
      * 【一个用户】【一天内】对【某个广告】点击的次数超过了【100次】，这样的用户属于黑名单用户
      *
      *
      * zhangsan:
      *          A:50  B:60
      * lisi:
      *          A:50   A:20  A:40   这就是黑名单用户
      * 如果一个用户今天是黑名单用户，那么明天还是黑名单用户吗？
      * 这个看业务而定。
      *
      * 第三步：动态生成黑名单  实时生成黑名单
      */
    DynamicGenerationBlacklists(filterLogDStream,spark)

    /**
      * 第四步：
      *        实时统计每天各省各城市广告点击量
      */
    val dateProvinceCityAdvClick_Count = ProvinceCityAdvClick_Count(filterLogDStream)
    /**
      * 第五步：
      *       实时统计每天各省热门广告
      *        分组求TopN
      *
      *   transform  froeachRDD
      *   rdd   => dataframe
      *   SparkSQL:
      *     SQL
      */


    /**
      * 第六步：
      *     实时统计每天每个广告在最近一小时的滑动窗口的点击趋势
      */

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  /**
    * 对黑名单数据进行过滤
    * @param logDstream  从kafka读取数据
    * @return  进行黑名单过滤以后的数据
    */
  def blackListFilter(logDstream: DStream[String],ssc:StreamingContext):DStream[String]={

    /**
      * 这个地方应该是去数据库里面去读取数据
      * black_list
      */

    val blackList = List((1L,true),(2L,true),(3L,true))
    val blackListRDD = ssc.sparkContext.parallelize(blackList)
    val balckListBroadcast = ssc.sparkContext.broadcast(blackListRDD.collect()) //conllect：在本地节点上遍历rdd的所有元素

    /**
      * 这个地方的黑名单，应该是从我们的持久化的数据库里面读取的：有三个数据库是我们常用的：
      * 1）Reids   自己去百度一下
      * 2) HBase  自己去百度一下
      * 3) Mysql  上课演示过
      * SparkCore的方式读取的
      * SparkSQL  -> dataframe -> rdd
      */

    logDstream.transform( rdd =>{
      val user_lineRDD=rdd.map( line =>{
        val fields = line.split(",")
        (fields(3).toLong,line)
      })
      val blackRDD = rdd.sparkContext.parallelize(balckListBroadcast.value)
      val resultRDD: RDD[(Long, (String, Option[Boolean]))] = user_lineRDD.leftOuterJoin(blackRDD)
      resultRDD.filter( tuple =>{
        tuple._2._2.isEmpty
      }).map(_._2._1)

    })
  }

  /**
    * 动然生成黑名单
    * @param filterLogDStream  黑名单过滤万了以后的数据
    * 【一个用户】【一天内】对【某个广告】点击的次数超过了【100次】，这样的用户属于黑名单用户
    *
    * 梳理一下思路：
    *   这个需求 跟 我们单词计数很像，无非不就是实时统计每个单词出现了多少次
    *   如果发现某个单词出现了一个100，那么他就是黑名单单词
    *   方式一：
    *   (date_userid_advid,v)=map
    *    实时统计出来每个单词出现了多少次=updateStateBykey （对内存的要求高一点）
    *    张三 A 80
    *    李四 B 99
    *         100
    *    fitler  过滤出来次数 一百以上 把它写入 MySQL，Reids,HBase 数据库
    *   方式二：
    *   (date_userid_advid,v)=map
    *    每次处理的是本批次的数据 reduceBykey(对内存的要求低一点)
    *    HBase:
    *        rowkey:  date_userid_advid  2
    *          本批次  3
    *            5
    *    Redis
    *   方式三：
    *        MySQL的方式
    *
    *
    *
    *
    */
  def DynamicGenerationBlacklists(filterLogDStream: DStream[String],spark:SparkSession):Unit={
    val date_userid_advid_ds=filterLogDStream.map( line =>{
      val fields = line.split(",")
      val time = new Date( fields(0).toLong)
      val date = DateUtils.formatDateKey(time)
      val userid = fields(3)
      val advid = fields(4)
      //20180512_
      (date+"_"+userid+"_"+advid,1L)
    }).reduceByKey(_+_)

    date_userid_advid_ds.foreachRDD( rdd =>{
      rdd.foreachPartition( partition =>{
        val connection = ConnectionPool.getConnection()
        val statement = connection.createStatement()
        partition.foreach{
          case(date_userid_advid,count) =>{
            val fields = date_userid_advid.split("_")
            val date = fields(0)
            val userid = fields(1).toLong
            val advid = fields(2).toLong
            val sql=s"insert into aura.tmp_advclick_count values($date,$userid,$advid,$count)";
            statement.execute(sql);
          }
        }
        ConnectionPool.returnConnection(connection)

      })
    })

    /**
      *生成黑名单
      */

    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/aura")
      .option("user", "aura")
      .option("password", "aura")
      .option("dbtable", "tmp_advclick_count")
      .load()

    df.createOrReplaceTempView("tmp_advclick_count")

    val sql=
      """
         SELECT
              userid
         FROM
         (
         SELECT
              date,userid,advid,sum(click_count) c_count
              FROM
              tmp_advclick_count
         GROUP BY
              date,userid,advid
         ) t
              WHERE
              t.c_count > 100
      """

    //统计出来黑名单
    val blacklistdf = spark.sql(sql).distinct()
    val properties = new Properties()
    properties.put("user","aura")
    properties.put("password","aura")
    blacklistdf.write.mode(SaveMode.Append)
      .jdbc("jdbc:mysql://localhost:3306/aura","black_list",properties)
  }

  /**
    * 实时统计每天各省各城市广告点击量
    * @param filterLogDStream
    */
  def ProvinceCityAdvClick_Count(filterLogDStream: DStream[String]):DStream[(String,Long)]={
    /**
      * 思路
      * map  => (k,v)  => date+province+city+advid  1
      *                updateStateBykey
      */
    var f=(input:Seq[Long],state:Option[Long]) =>{
      val current_count = input.sum
      val last_count = state.getOrElse(0)
      Some(current_count+last_count)
    }

    filterLogDStream.map( line =>{
      val fields = line.split(",")
      val time = fields(0).toLong
      val mydate = new Date(time)
      val date = DateUtils.formatDateKey(mydate)
      val province = fields(1)
      val city = fields(2)
      val advid = fields(4)
      (date+"_"+province+"_"+city+"_"+advid,1L)
    }).updateStateByKey(f)
    /**
      * 如果开发有需求的话，可以把这些数据库写入 MySQL数据库 ，Hbase
      */
  }

  /**
    * 实时统计 各省热门广告
    *
    * transform : rdd  -> datafram  -> table -> sql
    * @param date_province_city_advid_count
    */
  def ProvinceAdvClick_Count(date_province_city_advid_count:DStream[(String,Long)],spark:SparkSession): Unit ={
    date_province_city_advid_count.transform( rdd =>{
      var date_province_advid_count=  rdd.map{
        case(date_province_city_advid,count) =>{
          val fields = date_province_city_advid.split("_")
          val date = fields(0)
          val province = fields(1)
          val advid = fields(3)


          (date+"_"+province+"_"+advid,count)
        }
      }.reduceByKey(_+_)

      val rowRDD=date_province_advid_count.map( tuple =>{
        val fields = tuple._1.split("_")
        val date = fields(0)
        val provnice = fields(1)
        val advid = fields(2).toLong
        val count = tuple._2
        Row(date,provnice,advid,count)
      })

      val schema=StructType(
        StructField("date",StringType,true)::
          StructField("province",StringType,true)::
          StructField("advid",LongType,true)::
          StructField("count",LongType,true):: Nil

      )

      val df = spark.createDataFrame(rowRDD,schema)

      df.createOrReplaceTempView("temp_date_province_adv_count")

      val sql=
        """
           select
                *
           from
           (
           select
                date,province,advid,count,row_number() over(partition by province ordr by count desc) rank
           from
                temp_date_province_adv_count
           ) temp
           where temp.rank < 10

        """

      /**
        * 把结果持久化到数据库
        */
      spark.sql(sql)

      rdd

    })



  }

}

