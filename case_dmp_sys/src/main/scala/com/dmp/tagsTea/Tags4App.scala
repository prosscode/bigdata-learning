package com.dmp.tagsTea

import com.dmp.entity.Logs
import org.apache.commons.lang.StringUtils

/**
  * Created by Administrator on 2018/5/16.
  */
object Tags4App extends  Tags{
  /**
    * 打标签的方法
    * 给APP打标签
    * @param args
    *   args0:Logs
    *   args1:Map[String,String]:
    *           key:appID
    *           value:appName
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map=Map[String,Int]()
    if(args.length > 1){
      val log = args(0).asInstanceOf[Logs]
      val appDict = args(1).asInstanceOf[Map[String,String]]
      val appName = appDict.getOrElse(log.appid,log.appname)
      if(StringUtils.isNotEmpty(appName) && !"".equals(appName)){
         map += ("APP"+appName -> 1)
      }
    }
    map
  }
}
