package com.dmp.report

import com.dmp.entity.Logs
import com.dmp.utils.ReportUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/5/15.
  */
object AppReport {
  def main(args: Array[String]): Unit = {
    /**
      * 1) 判断参数个数
      */
    if(args.length < 3){
       println(
         """
           |com.dmp.report.AppReport <logDataPath> <appMappingPath> <outputPath>
           | <logDataPath> 日志目录
           | <appMappingPath> 映射文件目录
           | <outputPath> 输出结果文件目录
         """.stripMargin)
      System.exit(0)
    }
    /**
      * 2）接收参数
      */
    val Array(logDataPath,appMappingPath,outpoutPath)=args
    /**
      * 3) 初始化程序入口
      */
     val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName}")
    conf.setMaster("local")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Logs]))
    val sc = new SparkContext(conf)
    /**
      * 4) 把APP映射文件作为广播变量
      */
    val appMap: Map[String, String] = sc.textFile(appMappingPath).flatMap(line => {
      import scala.collection.mutable.Map
      val map = Map[String, String]()
      val fields = line.split("\t")
      map += (fields(4) -> fields(1))
      map
    }).collect().toMap

    val broadcastAppMap = sc.broadcast(appMap)

    /**
      * 5) 生成报表
      */
    sc.textFile(logDataPath).map( line =>{
      val log = Logs.line2Log(line)
      //统计请求数(总请求，有效请求，广告请求)
      val adRequest = ReportUtils.calculateRequest(log)
      //统计竞价数(参与竞价数和竞价成功数)
      val adResponse = ReportUtils.calculateResponse(log)
      //计算点击量，展示量
      val adClick = ReportUtils.calculateShowClick(log)
      //计算广告消费和广告成本
      val adCost = ReportUtils.calculateAdCost(log)
      //统计的媒体APP
      val appName = broadcastAppMap.value.getOrElse(log.appid,log.appname)
      (appName,adRequest ++ adResponse ++ adClick ++ adCost)
    }).filter( tuple =>{
      tuple._1.nonEmpty && !"".equals(tuple._1 )
    }).reduceByKey{
      case(list1, list2) =>{
        list1.zip(list2).map{
          case (x,y) => x + y
        }
      }
    }.foreach( tuple =>{
      val appName = tuple._1
      val report = tuple._2.mkString(",")
      println(appName + " "+ report)
    })


    sc.stop()

  }

}
