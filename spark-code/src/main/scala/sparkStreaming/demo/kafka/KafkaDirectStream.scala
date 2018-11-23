package sparkStreaming.demo.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017/9/17.
  */
object KafkaDirectStream {
  def main(args: Array[String]): Unit = {
    val group="1710"
    val conf = new SparkConf().setAppName("KafkaDirectStream").setMaster("local[2]")

    val ssc = new StreamingContext(conf,Seconds(5))

    val topic="test"
    val brokerList="hadoop1:9092"

    val zkQuorum="hadoop1:2182,hadoop1:2183,hadoop1:2184"

    val topics = Set(topic)
    //创建一个对象， 其实是指定往zk写入数据的目录，用于保存偏移量
    val topicDirs = new ZKGroupTopicDirs(group,topic)  // 1710/test     zookeeper类似于一个文件系统，kafka的数据存在目录里面
    //获取zk中的路径
    val zkPath = topicDirs.consumerOffsetDir   // 1710/test

    val kafkaPrams = Map(
      "metadata.broker.list" -> brokerList,
      "group.id" -> group
    )

    val zKClient = new ZkClient(zkQuorum)
    //查看该路径下是否有子节点（不同的分区保存不同的offset）
    val children = zKClient.countChildren(zkPath)  //      /1709/aura/

    /**
      * /1710/aura/0  200
      * /1710/aura/1  190
      * /1710/aura/2  190
      *
      */
    //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    var kafkaStream:InputDStream[(String, String)]=null
    if(children > 0){
      for(i <- 0 until children){
         //获取分区里面的数据   也就是偏移量
        val partitionOffset = zKClient.readData[String](s"${zkPath}/${i}")  ///1709/aura/0

         val tp = TopicAndPartition(topic,i)
        //aura/0  -> 1000  将不同partition对应的offset 增加到fromOffsets中
        fromOffsets +=(tp -> partitionOffset.toLong)
      }

      /**
        *
        * [K, V, KD <: Decoder[K], VD <: Decoder[V], R]
        * (
        * ssc: StreamingContext,
        * kafkaParams: Map[String, String],
        * fromOffsets: Map[TopicAndPartition, Long],
        * messageHandler: (MessageAndMetadata[K, V]) ⇒ R
        * )
        */
      //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())

       kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
        ssc, kafkaPrams, fromOffsets, messageHandler)

      kafkaStream
    }else{
      //如果未保存，根据 kafkaParam 的配置使用最新或者最旧的 offset
      kafkaStream=KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaPrams, topics)
    }
    var offsetRanges = Array[OffsetRange]()

    //业务代码
    kafkaStream.transform( rdd =>{
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }).map( msg => msg._2)  //业务代码的开发
      .foreachRDD( rdd => {
        rdd.foreachPartition( parition =>{
          parition.foreach( recoder =>{
            println(recoder)
          })
        })
          //更新偏移量
        for( o <- offsetRanges){
          val newZkPath = s"${zkPath}/${o.partition}"
          //将该 partition 的 offset 保存到 zookeeper
          ZkUtils.updatePersistentPath(zKClient, newZkPath, o.fromOffset.toString)
        }
      })

    println("xxxxxxxxxxxxxxxxxxxxxxxxxxx")


    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

}
