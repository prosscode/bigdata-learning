package com.dmp.tagsTea

import com.dmp.entity.Logs
import com.dmp.utils.Utils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/5/16.
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    //判断参数
    if(args.length < 4){
      println(
        """
          |com.dmp.tags.TagsContext
          |<inputLogPath> 输入的日志文件路径
          |<appMappingPath> app映射文件路径
          |<deviceMappingPath>设备的映射文件路径
          |<outputPath> 输出的结果文件存储
        """.stripMargin)
      System.exit(0)
    }
     //接收参数
    val Array(inputPath,appPath,devicePath,outputPath)=args
    //初始化对象
    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName}")
    conf.setMaster("local")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Logs]))
     //初始化程序入口
    val sc = new SparkContext(conf)
    //生成 APP 广播 变量
    val appMap: Map[String, String] = sc.textFile(appPath).flatMap(line => {
      var map = Map[String, String]()
      val fields = line.split("\t")
      if (fields.length > 4) {
        map += (fields(4) -> fields(1))
      }
      map
    }).collect().toMap

    val appMapBroadcast = sc.broadcast(appMap)

    //生成设备广播变量
    val deviceMap: Map[String, String] = sc.textFile(devicePath).map(line => {
      var map = Map[String, String]()
      val fields = line.split("\t")
      if (fields.length > 1) {
        map += (fields(0) -> fields(1))
      }
      map
    }).collect.flatten.toMap

    val deviceMapBroadcast = sc.broadcast(deviceMap)

    //进行打标签
    sc.textFile(inputPath).map( line =>{
      val log = Logs.line2Log(line)
      val localTag = Tags4Local.makeTags(log)
      val appTag = Tags4App.makeTags(log,appMapBroadcast.value)
      val channelTag = Tags4Channel.makeTags(log)
      val deviceTag = Tags4Device.makeTags(log,deviceMapBroadcast.value)
      val keyWordsTag = Tags4KeyWords.makeTags(log)
      val areaTag = Tags4Area.makeTags(log)

      val userid = getNotEmptyID(log).getOrElse("")
      (userid,(localTag ++  appTag ++ channelTag  ++ deviceTag ++ keyWordsTag ++ areaTag).toList)
    }).filter(!_._1.toString.equals(""))
      .reduceByKey{
        case(list1,list2) =>{
          (list1 ++ list2).groupBy(_._1)
              .map{
                case(k,list) =>{
                  (k,list.map( t =>  t._2).sum)
                }
              }.toList
        }
      }.foreach( tuple =>{

      println(tuple._1  + "->"+ tuple._2.mkString("\t"))

    })

    sc.stop()
  }

  // 获取用户唯一不为空的ID
  def getNotEmptyID(log: Logs): Option[String] = {
    log match {
      case v if v.imei.nonEmpty => Some("IMEI:" + Utils.formatIMEID(v.imei))
      case v if v.imeimd5.nonEmpty => Some("IMEIMD5:" + v.imeimd5.toUpperCase)
      case v if v.imeisha1.nonEmpty => Some("IMEISHA1:" + v.imeisha1.toUpperCase)

      case v if v.androidid.nonEmpty => Some("ANDROIDID:" + v.androidid.toUpperCase)
      case v if v.androididmd5.nonEmpty => Some("ANDROIDIDMD5:" + v.androididmd5.toUpperCase)
      case v if v.androididsha1.nonEmpty => Some("ANDROIDIDSHA1:" + v.androididsha1.toUpperCase)

      case v if v.mac.nonEmpty => Some("MAC:" + v.mac.replaceAll(":|-", "").toUpperCase)
      case v if v.macmd5.nonEmpty => Some("MACMD5:" + v.macmd5.toUpperCase)
      case v if v.macsha1.nonEmpty => Some("MACSHA1:" + v.macsha1.toUpperCase)

      case v if v.idfa.nonEmpty => Some("IDFA:" + v.idfa.replaceAll(":|-", "").toUpperCase)
      case v if v.idfamd5.nonEmpty => Some("IDFAMD5:" + v.idfamd5.toUpperCase)
      case v if v.idfasha1.nonEmpty => Some("IDFASHA1:" + v.idfasha1.toUpperCase)

      case v if v.openudid.nonEmpty => Some("OPENUDID:" + v.openudid.toUpperCase)
      case v if v.openudidmd5.nonEmpty => Some("OPENDUIDMD5:" + v.openudidmd5.toUpperCase)
      case v if v.openudidsha1.nonEmpty => Some("OPENUDIDSHA1:" + v.openudidsha1.toUpperCase)

      case _ => None
    }

  }

}
