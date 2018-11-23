package RPC

/**
  * Author: shawn pross
  * Date: 2018/04/25
  * Description: 
  */
trait Message

case class registerMessage(var hostname:String,var username:String,var password:String)
case class heartbeatMessage(var hostname:String)
