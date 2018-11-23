package com.dmp.tagsTea

import com.dmp.entity.Logs

/**
  * Created by Administrator on 2018/5/16.
  */
object Tags4Device extends  Tags{
  /**
    * 打标签的方法
    * 设备标签：
    * 1）设备操作系统
    * 2）设备联网方式标签
    * 3）设备运营商方案标签
    * @param args
    *          args0:Logs
    *          args1:Map[String,String]
    *          key:WIFI
    *          value: D00020001
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map=Map[String,Int]()
    if(args.length > 1){
      val log = args(0).asInstanceOf[Logs]
      val deviceDict = args(1).asInstanceOf[Map[String,String]]
      //操作系统标签
      val os = deviceDict.getOrElse(log.client.toString,deviceDict.get("4").get)
      map += (os -> 1)
      //联网方式标签
      val network = deviceDict.getOrElse(log.networkmannername,deviceDict.get("NETWORKOTHER").get)
       map += (network -> 1)
      //运营商的标签
      val isp = deviceDict.getOrElse(log.ispname,deviceDict.get("OPERATOROTHER").get)
    }
    map
  }
}
