package actorrpc2

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

/**
  * Author: shawn pross
  * Date: 2018/04/19
  * Description:  服务端
  */
class Server extends Actor{
	val arrays=new Array(10)
	//处理判定是注册的消息
	def handleRegister(array:Array[String]): Unit ={
//		arrays(array)
	}
	//处理判定是心跳的消息
	def handleHeartBeat(hostname:String) = {
//		arrays(hostname)
	}

	override def receive: Receive = {
		//如果是接收注册消息
		case registerMessage(hostname,memorySize,cpu)=>{
			val array = Array(hostname,memorySize,cpu)
			handleRegister(array)
			sender() ! "receiveRegister"
		}
			case heartbeatMessage(hostname)=>{
				handleHeartBeat(hostname)
				sender()!"receiveHeart"
			}
	}
}
object Server {
	def main(args: Array[String]): Unit = {
		val str=
			"""
			  |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
			  |akka.remote.netty.tcp.hostname =localhost
			  |akka.remote.netty.tcp.port=8888
			""".stripMargin
		val conf: Config = ConfigFactory.parseString(str)
		// def apply(name: String, config: Config)
		val actorSystem = ActorSystem("ServerActorSystem",conf)
		//创建并启动actor   def actorOf(props: Props, name: String): ActorRef
		actorSystem.actorOf(Props(new Server()),"ServerActor")
	}

}
