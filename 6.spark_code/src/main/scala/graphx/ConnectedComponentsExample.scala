package graphx

import org.apache.spark.graphx.{GraphLoader, VertexId, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/5/17.
  */
object ConnectedComponentsExample {
  def main(args: Array[String]): Unit = {
    //graphx 基于RDD
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("ConnectedComponentsExample")
    val sc = new SparkContext(conf)

      /**
        * 构建出来图有多种方式
        * followers.txt边数据：只有源顶点和目标顶点，中间以空格隔开,多余的列无用，
        * 如：2 1 other 有3列数据，但是graphx只会读取前两列
        */
    //加载边时顶点是边上出现的点
    val grapxh = GraphLoader.edgeListFile(sc,"E:\\BigData\\14_spark\\day15_graphx\\followers.txt")
//    grapxh.vertices.foreach(println(_))
      /**
        * (4,1)
        * (1,1)
        * (6,1)
        * (3,1)
        * (7,1)
        * (2,1)
        */
    val cc: VertexRDD[VertexId] = grapxh.connectedComponents().vertices

      /**
        * 加载结果数据
        * 解析顶点数据:ID(一定转成Long型)
        */
    val users = sc.textFile("E:\\BigData\\14_spark\\day15_graphx\\users.txt").map(line => {
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    })

    //1,刘德华 join   1,1
    //1 刘德华，1（代表的是同一个好友的那个id）
    users.join(cc).map{
      case(id,(username,cclastid)) =>(cclastid,username)
    }.reduceByKey( (x:String,y:String) => x + ","+ y)
      .foreach(tuple =>{
        println(tuple._2)
      })
    sc.stop()
  }
}
