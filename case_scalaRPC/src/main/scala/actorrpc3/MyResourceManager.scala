package actorrpc3

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import scala.collection.mutable

class MyResourceManager(var hostname: String, var port: Int) extends Actor {

	private var id2nodemanagerinfo = new mutable.HashMap[String, NodeManagerInfo]()
	private var nodemanagerInfoes = new mutable.HashSet[NodeManagerInfo]()


	override def preStart(): Unit = {
		import scala.concurrent.duration._
		import context.dispatcher
		context.system.scheduler.schedule(0 millis, 5000 millis, self, CheckTimeOut)
	}

	override def receive: Receive = {
		case RegisterNodeManager(nodemanagerid, memory, cpu) => {
			val nodeManagerInfo = new NodeManagerInfo(nodemanagerid, memory, cpu)

			id2nodemanagerinfo.put(nodemanagerid, nodeManagerInfo)

			nodemanagerInfoes += nodeManagerInfo

			//把信息存到zookeeper
			sender() ! RegisteredNodeManager(hostname + ":" + port)

		}

		case Heartbeat(nodemanagerid) => {
			val currentTime = System.currentTimeMillis()
			val nodeManagerInfo = id2nodemanagerinfo(nodemanagerid)
			nodeManagerInfo.lastHeartBeatTime = currentTime

			id2nodemanagerinfo(nodemanagerid) = nodeManagerInfo
			nodemanagerInfoes += nodeManagerInfo

		}

		case CheckTimeOut => {
			val currentTime = System.currentTimeMillis()

			//      for(nm <- nodemanagerInfoes){
			//        val time = nm.lastHeartBeatTime
			//        if(currentTime - time > 15000){
			//          nodemanagerInfoes -= nm
			//          id2nodemanagerinfo.remove(nm.nodemanagerid)
			//        }
			//      }

			nodemanagerInfoes.filter(nm => currentTime - nm.lastHeartBeatTime > 15000)
					.foreach(deadnm => {
						nodemanagerInfoes -= deadnm
						id2nodemanagerinfo.remove(deadnm.nodemanagerid)
					})
			println("当前注册成功的节点数" + nodemanagerInfoes.size);
		}
	}
}

object MyResourceManager {
	def main(args: Array[String]): Unit = {
		val RESOURCEMANAGER_HOSTNAME = args(0) //解析的配置的日志
		val RESOURCEMANAGER_PORT = args(1).toInt
		val str =
			s"""
			   |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
			   |akka.remote.netty.tcp.hostname =${RESOURCEMANAGER_HOSTNAME}
			   |akka.remote.netty.tcp.port=${RESOURCEMANAGER_PORT}
      """.stripMargin

		val conf = ConfigFactory.parseString(str)
		val actorSystem = ActorSystem(Constant.RMAS, conf)
		actorSystem.actorOf(Props(new MyResourceManager(RESOURCEMANAGER_HOSTNAME, RESOURCEMANAGER_PORT)), Constant.RMA)

	}
}
