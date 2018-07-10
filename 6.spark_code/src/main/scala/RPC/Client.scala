package RPC

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

/**
  * Author: shawn pross
  * Date: 2018/04/25
  * Description:  客户端
  */
class Client extends Actor{

	def doReRegister(): Unit = {
		println("Client：服务器收到了注册消息")
	}


	def doReHeart(): Unit = {
		println("Client：服务器收到了心跳消息")
	}

	override def receive: Receive = {
		case "receiveRegister" =>{
			doReRegister()
		}
		case "receiveHeart" =>{
			doReHeart()
		}
	}

	override def preStart(): Unit = {
		//发送一个注册消息
		val rmref = context.actorSelection("akka.tcp://ServerActorSystem@localhost:8888/user/ServerActor")

		rmref ! registerMessage("client","pross","123456")

		rmref ! heartbeatMessage("localhost")

	}
}

object Client {
	def main(args: Array[String]): Unit = {
		val str=
			"""
			  |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
			  |akka.remote.netty.tcp.hostname = localhost
			""".stripMargin
		val conf = ConfigFactory.parseString(str)
		val actorSystem = ActorSystem("CilentActorSystem",conf)
		actorSystem.actorOf(Props(new Client()),"CilentActor")
	}

}
