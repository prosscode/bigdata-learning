package com.dmp.tagsTea

import com.dmp.entity.Logs
import org.apache.commons.lang.StringUtils

/**
  * Created by Administrator on 2018/5/16.
  */
object Tags4KeyWords  extends  Tags{
  /**
    * 打标签的方法
    * 打关键字的标签
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] ={
    var map=Map[String,Int]()
    if(args.length > 0){
       val log = args(0).asInstanceOf[Logs]
      if(StringUtils.isNotEmpty(log.keywords)){
        val fields = log.keywords.split("\\|")
//        for(word <- fields){
//          if(word.length >= 3 && word.length <= 8){
//            map +=("K".concat(word) -> 1)
//          }
//        }
        fields.filter( word =>{
          word.length >=3 && word.length <=8
        }).map( str =>{
         map +=("K".concat(str.replace(":",""))  -> 1)
        })
      }
    }
    map
  }
}
