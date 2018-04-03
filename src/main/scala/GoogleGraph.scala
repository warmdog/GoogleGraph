
import scala.collection.mutable.Map
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable
object GoogleGraph extends Serializable  {
  val initialMsg = Iterator[(node,Int)]()
  val u1 = 1.0
  val u2 = 0.01
  val u3 = 0.01
  //标签个数 标签默认少于10000
  val m = 4
  // 默认保存的 top k 标签
  val k =3
  // 更新每个标签权重 默认为1
  val yvl = Map[Long,Double](3L->1.0,2L->1.0, 3L -> 1.0,6L -> 1.0)
  val maxLabel = 10

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val graph = initialGraph()
    graph.vertices.foreach(println(_))
    val test = graph.pregel(initialMsg,
      1,
      EdgeDirection.Both)(
      vprog,
      sendMsg,
      mergeMsg)
    test.vertices.foreach(println(_))
  }

  //设置默认U值先验概率分布
  def getU() :Double ={

    val res =1.0/m
    res

  }
    def vprog(vertexId: VertexId, value: node, message: Iterator[(node,Int)]): node = {
        if(message.isEmpty){value}
        else{
          val label= value.y.getOrElse(vertexId,0.0)

          val svv = if(label >=1.0) 1.0 else 0.0

          //val mvl = u1 * (svv) + u2 * message._2+ u3
          //val yvli = (u1 * svv * yvl.getOrElse()  )
          var sum_Mw = 0.0
          var map = Map[Long,Double]()

          while(message.hasNext){
            val attr = message.next()
            for((k,v) <- attr._1.y){
              val tmp = map.getOrElse(k,0.0)
              map.put(k,v *attr._2+tmp)
            }
            sum_Mw = sum_Mw + attr._2
          }

          val mvl = u1 * svv + u2 *sum_Mw +u3
          //val yvl = (u1 * svv * )
          val filter = value.y.filter(x => x._1!=vertexId)
          for((k,v) <- value.y){
            if(map.contains(k)){
              val tmp = map.getOrElse(k,0.0)
              val yvli = (u1 * svv * yvl.getOrElse(k,1.0/m) + u2 * tmp + u3 *getU()) *1.0 /mvl

              value.y.put(k,yvli)
            }
          }

          for((k,v) <- map){
            if(!value.y.contains(k)){
              val yvli = (u1 * svv * yvl.getOrElse(k,1.0/m) + u2 * v + u3 * getU()) *1.0 /mvl
              value.y.put(k,yvli)
            }
          }
        }
      value
    }
    def sendMsg(triplet: EdgeTriplet[node, Int]): Iterator[(VertexId, Iterator[(node,Int)])] = {
      val sourceVertex = triplet.srcAttr
      triplet.attr //边值
      // if (sourceVertex == sourceVertex._2)
      //  Iterator.emptyval
      // else
      //  Iterator((triplet.dstId, sourceVertex.toInt))
      val destY = triplet.dstAttr.y.filter( x => x._1<=10000)
      triplet.dstAttr.y = destY
      val srcY = triplet.srcAttr.y.filter( x => x._1<=10000)
      triplet.srcAttr.y = srcY
      Iterator((triplet.srcId, Iterator((triplet.dstAttr,triplet.attr))),(triplet.dstId, Iterator((triplet.srcAttr,triplet.attr))))
    }

    def mergeMsg(msg1: Iterator[(node,Int)], msg2: Iterator[(node,Int)]):Iterator[(node,Int)] = msg1 ++ msg2




  //初始化构图 不用分区 spark 默认分区
  def initialGraph(): Graph[node, Int] ={

    val spark = SparkSession.builder().master("local").appName("Spark GraphXXX").getOrCreate()
    val sc = spark.sparkContext
    val vertices: RDD[(VertexId, Long)] =
      sc.parallelize(Array((1L, 7L), (2L, 3L),
        (3L, 2L), (4L, 6L),(5L,0L),(6L,0L)))

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
      sc.parallelize(Array(Edge(1L, 2L, 1), Edge(1L, 4L, 1),
        Edge(2L, 4L, 2), Edge(3L, 1L, 3),
        Edge(3L, 4L, 1),Edge(4L,5L,2),Edge(3L,6L,2)))
    val g= Graph(verticesRDD, relationships)

    g
  }




}
