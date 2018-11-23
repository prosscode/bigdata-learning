package actorrpc

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

class MyNodeManager extends Actor{//生命周期
	def doHi(): Unit ={
	println("MyNodeManager：接收到了MyResourceManager的hi的消息");
	}
	//如果actor一执行首先运行的是这个方法，只运行一次。
	override def preStart(): Unit = {
		//实现的是给 MyResourcemanager 发送消息  地址
		val rmref = context.actorSelection("akka.tcp://MyResourceManagerActorSystem@localhost:19888/user/MyResourceManagerActor")
		rmref ! "hello"

	}

	override def receive: Receive = {
		case "hi"  => {
			doHi()
		}
	}
}
object MyNodeManager {
	def main(args: Array[String]): Unit = {

		val str=
			"""
			  |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
			  |akka.remote.netty.tcp.hostname = localhost
			""".stripMargin
		val conf = ConfigFactory.parseString(str)
		val actorySystem = ActorSystem("MyNodeManagerActorySystem",conf)
		actorySystem.actorOf(Props(new MyNodeManager()),"MyNodeManagerActory")



	}

}
