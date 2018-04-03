import scala.collection.mutable.Map

class node(index: Long, map: Map[Long,Double]) extends Serializable {
  var x: Long = index
  var y: Map[Long,Double] =map

}
//object test{
//  def main(args: Array[String]): Unit = {
//    val test  = new node(1,Map(11L ->10.0))
//    test.x = 20;
//    test.y =test.y .++ (Map(200L -> 10.0))
//    test.y.foreach(println(_))
//  }
//}