package com.dmp.test

import com.dmp.entity.Logs


/**
  * Created by Administrator on 2018/5/15.
  */
trait ReportData {
  def calculate(log:Logs):List[Double]
}
