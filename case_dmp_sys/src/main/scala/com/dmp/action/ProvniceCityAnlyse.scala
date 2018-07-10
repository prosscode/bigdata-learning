package com.dmp.action

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/5/15.
  */
object ProvniceCityAnlyse {
  def main(args: Array[String]): Unit = {
    /**
      * 第一步判断参数个数
      */
    if(args.length < 2){
      println(
        """
          |com.dmp.total.ProvniceCityAnlyse <inputFilePath><outputFilePath>
          |<inputFilePath> 输入是文件路径
          |<outputFilePath> 输出的文件路径
        """.stripMargin)
      System.exit(0)
    }

    /**
      * 第二步接收参数
      */
    val Array(inputFile,outputFile)=args
    /**
      * 第三步初始化程序入口
      */
    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName }")
    val spark=SparkSession.builder()
        .config(conf)
        .getOrCreate()
    /**
      * 第四步读取文件，进行业务逻辑开发
      * 云南省：
      * 云南省     曲靖市
      * 云南省     昆明市
      * 云南省     大理市
      */
    val df = spark.read.parquet(inputFile)
    df.createOrReplaceTempView("logs")
    val sql=
      """
         select
               count(*) ct,provincename,cityname
         from
              logs
         group by
              provincename,cityname
         order by
              provincename
      """

    /**
      * 第五步存储文件
      */
    spark.sql(sql).write.json(outputFile)
    spark.stop()
  }

}
