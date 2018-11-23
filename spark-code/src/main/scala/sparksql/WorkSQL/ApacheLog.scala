package sparksql.WorkSQL

/**
  * Author: shawn pross
  * Date: 2018/05/03
  * Description: 
  */
case class ApacheLog(ipAddress:String,
					client:String,
					 userID:String,
					 dateTime:String,
					 method:String, //请求方式
					 endPoint:String,
					 proptocal:String,
					 responseCode:Int,
					 contentSize:Double
					)

object ApacheLog {
	def parseLog(line:String): Unit ={
		val fields = line.split("\\#")
		val url = fields(4).split(" ")
		ApacheLog(
			fields(0),fields(1),fields(2),fields(3),
			url(0),url(1),url(2),
			fields(5).toInt,fields(6).toDouble
		)
	}
}
