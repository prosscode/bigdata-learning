package com.dmp.utils

import com.dmp.entity.Logs

/**
  * Created by Administrator on 2018/5/15.
  * 辅助报表开发
  */
object ReportUtils {
  /**
    * 统计请求数
    * @param log
    *  总请求，有效请求，广告请求
    */
  def calculateRequest(log:Logs): List[Double] ={
    if(log.requestmode == 1){
      if(log.processnode  == 1){
        List(1,0.0,0)
      }else if(log.processnode == 2){
        List(1,1,0)
      }else if(log.processnode == 3){
        List(1,1,1)
      }else{
        List(0,0,0)
      }

    }else{
      List(0,0,0)
    }
  }

  /**
    * 计算竞价数
    * @param log  日志对象
    * @return  参与竞价数和竞价成功数
    */
  def calculateResponse(log:Logs):List[Double]={
    if(log.adplatformproviderid >= 100000 && log.iseffective == 1 && log.isbilling == 1){
       if(log.isbid == 1 && log.adorderid !=0 ){
         List(1,0)
       }else if(log.iswin == 1){
         List(0,1)
       }else{
         List(0,0)
       }
    }else{
      List(0,0)
    }
  }

  /**
    * 计算展示量和点击量
    * @param log  输入的日志对象
    * @return  展示量  点击量
    */
  def calculateShowClick(log:Logs):List[Double]={
    if(log.iseffective == 1){
        if(log.requestmode == 2){
           List(1,0)
        }else if(log.requestmode == 3){
           List(0,1)
        }else{
          List(0,0)
        }
    }else{
      List(0,0)
    }

  }

  /**
    * 用于计算广告消费和广告成本
    * @param log
    * @return
    */
  def calculateAdCost(log:Logs):List[Double]={
    if(log.adplatformproviderid >= 100000
       && log.iseffective == 1
       && log.isbilling ==1
       && log.iswin ==1
       && log.adorderid >= 200000
       && log.adcreativeid >= 200000){
      List(log.winprice/1000,log.adpayment/1000)
    }else{
      List(0.0,0.0)
    }

  }

}
