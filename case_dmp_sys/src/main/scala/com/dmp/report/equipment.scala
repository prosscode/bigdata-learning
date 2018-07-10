package com.dmp.report

import com.dmp.entity.Logs
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

//val devicetype: Int, //设备类型（1：手机 2：平板）
object equipment {

  def main(args: Array[String]): Unit = {

    if(args.length < 1){
      println(
        """
           com.dmp.total.procityCount <inputpath>
        """.stripMargin)
      System.exit(0)
    }

    /**
      * 接受参数
      */
    val Array(inputpath)=args

    /**
      * 创建SparkSession对象
      */
    val conf = new SparkConf()
    conf.setAppName("procityCount").setMaster("local")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    //得到一个dateframe对象
    import spark.implicits._
    val df: DataFrame = spark.sparkContext.textFile(inputpath).map(line => Logs.line2Log(line)).toDF()
    df.createOrReplaceTempView("total")

    val sql=
      """
         select sum(总请求) 总请求,sum(有效请求) 有效请求,sum(广告请求) 广告请求,
         sum(参与竞价数) 参与竞价数,sum(竞价成功数) 竞价成功数,
         convert（varchar，cast（（竞价成功分数/参与竞价数）*100, as decimal(18,2))）+'%'
         as 竞价成功率,sum(展示量) 展示量，sum(点击量) 点击量,
         convert（varchar，cast（（点击量/展示量）*100, as decimal(18,2))）+'%'
         as 点击率,sum(广告成本) 广告成本，sum(广告消费) 广告消费 from phone
         (select case when requestmode=1 then 1
                case when processnode>=1 then 1
                else 0
                end as 总请求
                case when requestmode=1 then 1
                case when processnode>=2 then 1
                else 0
                end as  有效请求
                case when requestmode=1 then 1
                case when processnode=3 then 1
                else 0
                end as 广告请求
                case when adplatformproviderid >= 100000 then 1
                      case when iseffective =1 then 1
                      case when isbilling =1 then 1
                      case when isbid =1 then 1
                      case when adorderid <> 0 then 1
                      else 0
                      end as 参与竞价数
                case when adplatformproviderid >= 100000 then 1
                     case when iseffective =1 then 1
                     case when isbilling =1 then 1
                     case when iswin =1 then 1
                     else 0
                     end as 竞价成功数
                case when requestmode=2 then 1
                     case when iseffective=1 then 1
                     else 0
                     end as 展示数
                case when requestmode=3 then 1
                     case when iseffective=1 then 1
                     else 0
                     end as 点击数
                case when adplatformproviderid >=100000 then 1
                     case when iseffective=1 then 1
                     case when isbilling=1 then 1
                     case when iswin=1 then 1
                     case when adorderid>=200000 then 1
                     case when adcreativeid>=200000 then 1
                     else 0
                     end as 广告消费
                case when adplatformproviderid>=200000 then 1
                     case when iseffective=1 then 1
                     case when isbilling=1 then 1
                     case when iswin=1 then 1
                     case when adorderid>=200000 then 1
                     case when adcreativeid>=200000 then 1
                     else 0
                     end as 广告成本
                     from total where devicetype=1) phone
               UNION ALL
                select 总请求,有效请求,广告请求,参与竞价数,竞价成功数,
           convert（varchar，cast（（竞价成功分数/参与竞价数）*100, as decimal(18,2))）+'%'
           as 竞价成功率,展示量，点击量,
           convert（varchar，cast（（点击量/展示量）*100, as decimal(18,2))）+'%'
           as 点击率,广告成本，广告消费 from phone
           (select case when requestmode=1 then 1
                  case when processnode>=1 then 1
                  else 0
                  end as 总请求
                  case when requestmode=1 then 1
                  case when processnode>=2 then 1
                  else 0
                  end as  有效请求
                  case when requestmode=1 then 1
                  case when processnode=3 then 1
                  else 0
                  end as 广告请求
                  case when adplatformproviderid >= 100000 then 1
                      case when iseffective =1 then 1
                      case when isbilling =1 then 1
                      case when isbid =1 then 1
                      case when adorderid <> 0 then 1
                      else 0
                      end as 参与竞价数
                  case when adplatformproviderid >= 100000 then 1
                       case when iseffective =1 then 1
                       case when isbilling =1 then 1
                       case when iswin =1 then 1
                       else 0
                       end as 竞价成功数
                  case when requestmode=2 then 1
                       case when iseffective=1 then 1
                       else 0
                       end as 展示数
                  case when requestmode=3 then 1
                       case when iseffective=1 then 1
                       else 0
                       end as 点击数
                  case when adplatformproviderid >=100000 then 1
                       case when iseffective=1 then 1
                       case when isbilling=1 then 1
                       case when iswin=1 then 1
                       case when adorderid>=200000 then 1
                       case when adcreativeid>=200000 then 1
                       else 0
                       end as 广告消费
                  case when adplatformproviderid>=200000 then 1
                       case when iseffective=1 then 1
                       case when isbilling=1 then 1
                       case when iswin=1 then 1
                       case when adorderid>=200000 then 1
                       case when adcreativeid>=200000 then 1
                       else 0
                       end as 广告成本
                       from total where devicetype=2) pad

      """

  val dftemp = spark.sqlContext.sql(sql)
  }


}
