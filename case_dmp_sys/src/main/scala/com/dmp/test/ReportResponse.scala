package com.dmp.test

import com.dmp.entity.Logs


/**
  * Created by Administrator on 2018/5/15.
  */
class ReportResponse extends  ReportData {
  override def calculate(log: Logs): List[Double] = {
    if (log.adplatformproviderid >= 100000 && log.iseffective == 1 && log.isbilling == 1) {
      if (log.isbid == 1 && log.adorderid != 0) {
        List(1, 0)
      } else if (log.iswin == 1) {
        List(0, 1)
      } else {
        List(0, 0)
      }
    } else {
      List(0, 0)
    }
  }
}
