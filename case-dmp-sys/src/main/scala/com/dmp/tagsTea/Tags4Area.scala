package com.dmp.tagsTea

import com.dmp.entity.Logs
import org.apache.commons.lang.StringUtils

/**
  * Created by Administrator on 2018/5/16.
  */
object Tags4Area extends Tags{
  /**
    * 打标签的方法
    * 区域标签
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] ={
    var map=Map[String,Int]()
    if(args.length > 0){
       val log = args(0).asInstanceOf[Logs]
      if(StringUtils.isNotEmpty(log.provincename)){
        map += ("ZP"+log.provincename -> 1)
      }
      if(StringUtils.isNotEmpty(log.cityname)){
        map += ("ZC"+log.cityname -> 1)
      }
    }
    map
  }
}
