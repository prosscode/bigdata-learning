package actorrpc3

import java.util.UUID

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import sun.plugin2.message.HeartbeatMessage

class MyNodeManager(val resourcemanagerhostname: String,
					val resourcemanagerport: Int, val memory: Int, val cpu: Int) extends Actor {
	var nodemanagerid: String = _
	var rmRef: ActorSelection = _

	override def preStart(): Unit = {
		rmRef = context.actorSelection(s"akka.tcp://${Constant.RMAS}@${resourcemanagerhostname}:${resourcemanagerport}/user/${Constant.RMA}")
		//al nodemanagerid:String,val memory:Int,val cpu:Int
		nodemanagerid = UUID.randomUUID().toString
		rmRef ! RegisterNodeManager(nodemanagerid, memory, cpu)
	}

	override def receive: Receive = {
		case RegisteredNodeManager(masterURL) => {
			println(masterURL);

			// sender() ! Heartbeat(nodemanagerid)

			/**
			  * initialDelay: FiniteDuration, 多久以后开始执行
			  * interval:     FiniteDuration, 每隔多长时间执行一次
			  * receiver:     ActorRef, 给谁发送这个消息
			  * message:      Any  发送的消息是啥
			  */
			import scala.concurrent.duration._

			//context.system.scheduler.schedule(0 millis,1000 millis,sender(),Heartbeat(nodemanagerid))

			println(Thread.currentThread().getId)
			import context.dispatcher
			context.system.scheduler.schedule(0 millis, 4000 millis, self, SendMessage)
		}

		case SendMessage => {
			///
			//向主节点发送心跳信息
			rmRef ! Heartbeat(nodemanagerid)

			println(Thread.currentThread().getId)

		}

	}
}

object MyNodeManager {
	def main(args: Array[String]): Unit = {
		val HOSTNAME = args(0)
		val RM_HOSTNAME = args(1)
		val RM_PORT = args(2).toInt
		val NODEMANAGER_MEMORY = args(3).toInt
		val NODEMANAGER_CORE = args(4).toInt
		val NODEMANAGER_PORT = args(5).toInt
		val str =
			s"""
			   |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
			   |akka.remote.netty.tcp.hostname =${HOSTNAME}
			   |akka.remote.netty.tcp.port=${NODEMANAGER_PORT}
      """.stripMargin
		val conf = ConfigFactory.parseString(str)
		val actorSystem = ActorSystem(Constant.NMAS, conf)
		actorSystem.actorOf(Props(new MyNodeManager(RM_HOSTNAME, RM_PORT, NODEMANAGER_MEMORY, NODEMANAGER_CORE)), Constant.NMA)


	}
}