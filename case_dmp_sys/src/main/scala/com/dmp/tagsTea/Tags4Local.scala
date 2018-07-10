package com.dmp.tagsTea

import com.dmp.entity.Logs


/**
  * Created by Administrator on 2018/5/16.
  */
object Tags4Local extends  Tags{
  /**
    * 打标签的方法
    * 广告位的标签
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map=Map[String,Int]()
    if(args.length > 0){
      val log = args(0).asInstanceOf[Logs]
     if(log.adspacetype != 0 && log.adspacetype != null){
       log.adspacetype match{
         case x if x < 10 => map +=("LC0"+x -> 1)
         case x if x > 9  =>  map +=("LC"+x -> 1)
       }

     }
    }
    map
  }
}
