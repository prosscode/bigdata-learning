package com.dmp.tagsTea

/**
  * Created by Administrator on 2018/5/16.
  */
trait Tags {
  /**
    * 打标签的方法
    * @param args
    * @return
    */
  def makeTags(args:Any*):Map[String,Int]

}
