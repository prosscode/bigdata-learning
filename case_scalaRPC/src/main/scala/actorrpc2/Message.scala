package actorrpc2

/**
  * Author: shawn pross
  * Date: 2018/04/19
  * Description: 
  */
trait Message

case class registerMessage(var hostname:String,var memorySize:String,var cpu:String)
case class heartbeatMessage(var hostname:String)
