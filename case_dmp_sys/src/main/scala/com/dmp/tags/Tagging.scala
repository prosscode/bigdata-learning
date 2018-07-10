package com.dmp.tags

import com.dmp.entity.Logs
import com.dmp.utils.Utils

import scala.collection.mutable.Map

/**
  * Author: shawn pross
  * Date: 2018/05/15
  * Description:   用户画像工具类，打标签
  */
object Tagging {

	def makeTag(logs: Any*): Unit ={

	}

	/**
	  * 给广告位类型打上标签
	  * 标签格式：LC01-1   LC11-1
	  * @param logs
	  * @return
	  */
	def advTypeTag(logs: Logs): Map[String,Int] ={
		//可变集合Map
		val advTypeMap=Map[String,Int]()
		val advType: Int = logs.adspacetype
		//进行类型判断
		if(logs.adspacetype != 0 && logs.adspacetype != null){
			logs.adspacetype match{
				case x if x < 10 => advTypeMap +=("LC0"+x -> 1)
				case x if x > 9  =>  advTypeMap +=("LC"+x -> 1)
			}
		}
		advTypeMap
	}

	/**
	  * 给APP名称打上标签
	  * 格式：APP+XXX -> 1
	  * @param logs
	  * @return
	  */
	def appNameTag(logs: Logs): Map[String,Int] ={
		val appnameMap=Map[String,Int]()
		val appname: String = logs.appname
		val str: String = "APP".concat(appname)
		appnameMap+=(appname->1)
	}

	/**
	  * 给渠道打上标签
	  * 格式：CN+xxx -> 1
	  * @param logs
	  * @return
	  */
	def channelTag(logs: Logs): Map[String,Int] ={
		val channelTag=Map[String,Int]()
		val channelid: String = logs.channelid
		val str: String = "CN".concat(channelid)
		channelTag+=(str->1)
	}

	/**
	  * 关键字，不能少于3个字符，不超过8个字符
	  * @param logs
	  * @return
	  */
	def keywordTag(logs: Logs): Map[String,Int] ={
		var map=Map[String,Int]()
		logs.keywords.split("|").filter(word=>{
			word.length>=3 && word.length<=8
		}).map(str=>{
			map+=("K".concat(str.replace(":",""))->1)
		})
		map
	}

	/**
	  * 地域标签
	  * 格式：省(ZP+xxx -> 1) 市(ZC+xxx -> 1)
	  * @param logs
	  * @return
	  */
	def locationTag(logs: Logs): Array[Map[String,Int]] ={
		val provincename = logs.provincename
		val cityname = logs.cityname
		val provinceTag=Map("ZP".concat(provincename)->1)
		val cityTag=Map("ZC".concat(cityname)->1)
		Array(provinceTag,cityTag)

	}

	/**
	  * 获取用户唯一不为空的ID
	  * @param log
	  * @return
	  */
	def getNotEmptyID(log: Logs): Option[String] = {
		log match {
			case v if v.imei.nonEmpty => Some("IMEI:" + Utils.formatIMEID(v.imei))
			case v if v.imeimd5.nonEmpty => Some("IMEIMD5:" + v.imeimd5.toUpperCase)
			case v if v.imeisha1.nonEmpty => Some("IMEISHA1:" + v.imeisha1.toUpperCase)

			case v if v.androidid.nonEmpty => Some("ANDROIDID:" + v.androidid.toUpperCase)
			case v if v.androididmd5.nonEmpty => Some("ANDROIDIDMD5:" + v.androididmd5.toUpperCase)
			case v if v.androididsha1.nonEmpty => Some("ANDROIDIDSHA1:" + v.androididsha1.toUpperCase)

			case v if v.mac.nonEmpty => Some("MAC:" + v.mac.replaceAll(":|-", "").toUpperCase)
			case v if v.macmd5.nonEmpty => Some("MACMD5:" + v.macmd5.toUpperCase)
			case v if v.macsha1.nonEmpty => Some("MACSHA1:" + v.macsha1.toUpperCase)

			case v if v.idfa.nonEmpty => Some("IDFA:" + v.idfa.replaceAll(":|-", "").toUpperCase)
			case v if v.idfamd5.nonEmpty => Some("IDFAMD5:" + v.idfamd5.toUpperCase)
			case v if v.idfasha1.nonEmpty => Some("IDFASHA1:" + v.idfasha1.toUpperCase)

			case v if v.openudid.nonEmpty => Some("OPENUDID:" + v.openudid.toUpperCase)
			case v if v.openudidmd5.nonEmpty => Some("OPENDUIDMD5:" + v.openudidmd5.toUpperCase)
			case v if v.openudidsha1.nonEmpty => Some("OPENUDIDSHA1:" + v.openudidsha1.toUpperCase)

			case _ => None
		}
	}
}
