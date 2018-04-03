
import org.apache.commons.math3.util.IterationListener

import scala.collection.mutable.Map
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable
object GoogleTest extends Serializable  {
  val initialMsg = List[(node,Int)]()
  val u1 = 1.0
  val u2 = 0.01
  val u3 = 0.01
  //标签个数 标签默认少于10000
  val m = 3
  // 默认保存的 top k 标签
  val k =3
  // 更新每个标签权重 默认为1
  val yvl = Map[Long,Double](3L->1.0,2L->1.0, 3L -> 1.0,6L -> 1.0)
  val maxLabel = 10

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val graph = initialGraph()
    //graph.vertices.foreach(println(_))
    val test = graph.pregel(initialMsg,
      1,
      EdgeDirection.Both)(
      vprog,
      sendMsg,
      mergeMsg)

    test.vertices.foreach(x =>{
      val tmp =x._2.y.filter(k => k._1!=x._1)
      println(x._1)
      for((k,v) <- tmp){
        print( k + "  " + v +";")
      }
      println()

    })
    val test1 = test.pregel(initialMsg,
      4,
      EdgeDirection.Both)(
      vprog,
      sendMsg,
      mergeMsg)
    test1.vertices.foreach(x =>{
      val tmp =x._2.y.filter(k => k._1!=x._1)
      println(x._1)
      for((k,v) <- tmp){
        print( k + "  " + v +";")
      }
      println()

    })
  }

  //设置默认U值先验概率分布
  def getU() :Double ={

    val res =1.0/m
    res

  }
  def vprog(vertexId: VertexId, value: node, message: List[(node,Int)]): node = {

    if(message.size==0) {value}else {

      val label = value.y.getOrElse(vertexId, 0.0)

      val svv = if (label >= 1.0) 1.0 else 0.0

      //val mvl = u1 * (svv) + u2 * message._2+ u3
      //val yvli = (u1 * svv * yvl.getOrElse()  )
      var sum_Mw = 0.0
      var map = Map[Long, Double]()


      for ((k, v) <- message) {
        for ((m, n) <- k.y) {
          val tmp = map.getOrElse(m, 0.0)
          // 过滤0标签 0不是标签
          if(m!=0L){

           map.put(m, v * n + tmp)
          }
        }

        sum_Mw = sum_Mw + v
      }
      val newValue =new node(value.x,value.y);
      val mvl = u1 * svv + u2 * sum_Mw + u3
      //val yvl = (u1 * svv * )
      val filter = newValue.y.filter(x => x._1 != vertexId)
      for ((k, v) <- filter) {
        if (map.contains(k)) {
          val tmp = map.getOrElse(k, 0.0)
          val yvli = (u1 * svv * yvl.getOrElse(k, 1.0 / m) + u2 * tmp + u3 * getU()) * 1.0 / mvl

          newValue.y.put(k, yvli)
        }
      }

      for ((k, v) <- map) {
        if (!newValue.y.contains(k)) {
          val yvli = (u1 * svv * yvl.getOrElse(k, 1.0 / m) + u2 * v + u3 * getU()) * 1.0/mvl
          newValue.y.put(k, yvli)
        }
      }

      newValue
    }
  }
  def sendMsg(triplet: EdgeTriplet[node, Int]): Iterator[(VertexId, List[(node,Int)])] = {
    val sourceVertex = triplet.srcAttr
    triplet.attr //边值
    // if (sourceVertex == sourceVertex._2)
    //  Iterator.emptyval
    // else
    //  Iterator((triplet.dstId, sourceVertex.toInt))
    val destY = triplet.dstAttr.y.filter( x => x._1<=10000)
    val dstNode = new node(triplet.dstAttr.x,destY)
    val srcY = triplet.srcAttr.y.filter( x => x._1<=10000)
    val srcNode  = new node(triplet.srcAttr.x,srcY)
    Iterator((triplet.srcId, List((dstNode,triplet.attr))),(triplet.dstId, List((srcNode,triplet.attr))))
  }

  def mergeMsg(msg1:List[(node,Int)], msg2: List[(node,Int)]):List[(node,Int)] = msg2:::msg1

  //    val minGraph1 = graph.pregel(initialMsg,
  //      1,
  //      EdgeDirection.Out)(
  //      vprog,
  //      sendMsg,
  //      mergeMsg)
  //    println()
  //    val minGraph2 = minGraph1.pregel(initialMsg,
  //      1,
  //      EdgeDirection.Out)(
  //      vprog,
  //      sendMsg,
  //      mergeMsg)
  //    minGraph1
  //    println()
  //    test.vertices.foreach(x => {
  //      print(x._1 + "  ")
  //      println(x._2.y.toString())
  //    })


  //初始化构图 不用分区 spark 默认分区
  def initialGraph(): Graph[node, Int] ={

    val spark = SparkSession.builder().master("local").appName("Spark GraphXXX").getOrCreate()
    val sc = spark.sparkContext
    val vertices: RDD[(VertexId, Long)] =
      sc.parallelize(Array((100000L, 2L), (200000L, 3L),
        (300000L, 2L), (400000L, 6L),(500000L,0L),(600000L,0L)))

    val verticesRDD = vertices.map(x =>{
      //可以通过设置 weights 来更新 每个label的权重。默认都是1.0 yvl
      val weights  = yvl.getOrElse(x._2,1.0/m)

      // 标签id 和权重 ^Yvl
      val map = Map[Long,Double](x._2->weights)
      val nodeAtt = new  node(x._2,map)
      nodeAtt.y = nodeAtt.y.++(Map[Long,Double](x._1.toLong ->x._2.toDouble))
      (x._1,nodeAtt)
    })

    // Create an RDD for edges
    val relationships: RDD[Edge[Int]] =
      sc.parallelize(Array(Edge(100000L, 200000L, 1), Edge(100000L, 400000L, 1),
        Edge(200000L, 400000L, 2), Edge(300000L, 100000L, 3),
        Edge(300000L, 400000L, 1),Edge(400000L,500000L,1),Edge(300000L,600000L,2)))
    val g= Graph(verticesRDD, relationships)

    g
  }




}

