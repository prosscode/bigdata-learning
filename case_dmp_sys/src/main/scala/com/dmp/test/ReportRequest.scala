package com.dmp.test

import com.dmp.entity.Logs

/**
  * Created by Administrator on 2018/5/15.
  */
class ReportRequest extends ReportData{
  override def calculate(log: Logs): List[Double] = {
    if(log.requestmode == 1){
      if(log.processnode  == 1){
        List(1,0,0)
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
}
