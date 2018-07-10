package com.dmp.report

import com.dmp.entity.Logs
import com.dmp.utils.ReportUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/5/15.
  */
object ProvinceCityReport {

  def main(args: Array[String]): Unit = {
    if(args.length < 3){
      println(
        """
          |com.dmp.report.ProvinceCityReport <logInputPath> <provniceDataPath> <cityDataPath>
          |<logInputPath> 文件输入目录
          |<provniceDataPath> 省份结果文件目录
          | <cityDataPath> 城市结果文件目录
        """.stripMargin)
      System.exit(0)
    }
    val Array(loginputpath,provincedatapath,citydatapath)=args

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName(s"${this.getClass.getSimpleName}")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Logs]))
    val sc = new SparkContext(conf)

    val provinceCityRDD = sc.textFile(loginputpath).map(line => {
      val log = Logs.line2Log(line)
      val adRequest = ReportUtils.calculateRequest(log)
      val adResponse = ReportUtils.calculateResponse(log)
      val adClick = ReportUtils.calculateShowClick(log)
      val adCost = ReportUtils.calculateAdCost(log)
      (log.provincename, log.cityname, adRequest ++ adResponse ++ adClick ++ adCost)
    }).cache()


    /**
      * 省份的结果
      */

    provinceCityRDD.map( tuple =>{
      (tuple._1,tuple._3)
    }).reduceByKey{
      case(list1, list2) =>{
        list1.zip(list2).map{
          case (x,y) => x + y
        }
      }
    }.foreach( tuple =>{
      val provinceName = tuple._1
      val report = tuple._2.mkString(",")
      println(provinceName + " "+ report)
    })

    /**
      * 城市的结果
      */

    provinceCityRDD.map( tuple =>{
      (tuple._1 + tuple._2,tuple._3)
    }).reduceByKey{
      case(list1, list2) =>{
        list1.zip(list2).map{
          case (x,y) => x + y
        }
      }
    }.foreach( tuple =>{
      val provinceAndCityName = tuple._1
      val report = tuple._2.mkString(",")
      println(provinceAndCityName + " "+ report)
    })

    sc.stop()
  }

}
