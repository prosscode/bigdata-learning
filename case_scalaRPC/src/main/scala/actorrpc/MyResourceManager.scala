package actorrpc

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}
//akka.tcp://MyResourceManagerActorSystem@localhost:19888
class MyResourceManager extends  Actor{
	def doHello(): Unit ={
		println("MyResourceManager：接收到了MyNodeManager的hello的消息");
	}
	/**
	  * 其实就是一个死循环:接收消息
	  * while(true)
	  * @return
	  */
	override def receive: Receive = {
		case "hello" =>{
			doHello()
			//sender() 谁发送过来消息这个就是谁
			//sender()   ! "hi"  给sender()发送一个hi的消息
			sender() ! "hi"
		}
	}
}

object MyResourceManager {

	def main(args: Array[String]): Unit = {
		val str=
			"""
			  |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
			  |akka.remote.netty.tcp.hostname = localhost
			  |akka.remote.netty.tcp.port=19888
			""".stripMargin
		val conf: Config = ConfigFactory.parseString(str)
		// def apply(name: String, config: Config)
		val actorSystem = ActorSystem("MyResourceManagerActorSystem",conf)
		//创建并启动actor   def actorOf(props: Props, name: String): ActorRef
		//new MyResourceManager() 会导致主构造函数会运行！！
		actorSystem.actorOf(Props(new MyResourceManager()),"MyResourceManagerActor")

	}

}
