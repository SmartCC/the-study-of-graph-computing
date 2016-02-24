package smartCC.clustering

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{Graph, Edge,VertexId}
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.{pow, sqrt}

object KNN {
  //graph的边是double类型
  //graph[节点属性类型，边属性类型】
  //节点结构(vid,cid)
  def knn(graph:Graph[VertexId,Double], k:Int, N : Int, maxInter:Int, minDis : Double) = {
    //迭代次数
    var interNum : Int= 0
    //循环条件
    var notStop : Boolean = true

    //定义在函数里
    def mergeWithAndTakeN[K](m1: Map[K, List[Double]], m2: Map[K, List[Double]]): Map[K, List[Double]] = {
      (m1.toList:::m2.toList).
        groupBy(_._1).
        map {case (k,v)=> (k,v.map(_._2).reduce((a,b)=>a:::b).sortBy(-_).take(N))}
    }


    //takesample取随机点，三个参数分别为：【是否重复取值，取值个数，以及随机种子（测试使用，实际运行不要使用，否则每次结果都一样）】
    //随机种子为当前时间
    val kPoints = graph.vertices.sparkContext.parallelize(
      graph.vertices.takeSample(withReplacement = false,k,System.currentTimeMillis).map(t=>(t._1,t._1)))

    //graph中设置中心的为自身节点id，其余为None
    var subgraph = graph.outerJoinVertices(kPoints)((vid,vAttr,outerAttr)=> outerAttr)

    while(interNum < maxInter && notStop) {
      //过滤条件：1、两端都为空的保留。2、源数据端有类型，目的端为空的保留
      //原因：减少无用信息的发送
      subgraph = subgraph.subgraph(epred =
        edgeTriplet => ((edgeTriplet.srcAttr == None && edgeTriplet.dstAttr == None) ||
          (edgeTriplet.srcAttr != None && edgeTriplet.dstAttr == None))
      )

      //边是Double类型
      //发送消息，格式为Map (cid->List(score),...)
      val messages = subgraph.aggregateMessages[Map[Option[VertexId],List[Double]]](
        ctx=>if(ctx.srcAttr != None)
          ctx.sendToDst(Map(ctx.srcAttr->List(pow(ctx.attr,2)))),
        //(msg1,msg2)=> mergeWith(topN,msg1,msg2)
        //合并消息，取前N个
        (msg1,msg2) => mergeWithAndTakeN(msg1,msg2)
      ).map{
        //取距离点最大的聚类，输出格式为(节点id,(cid,距离))
        case(id,cidAnddis:Map[Option[VertexId],List[Double]])=>
          val nearestNeighbor = cidAnddis.map{ case(cid,dis) => (cid,sqrt(dis.sum)/dis.size)}.maxBy(_._2)
          (id,nearestNeighbor)}.
        filter{case(_,(_,dis)) => dis>=minDis} //过滤掉小于阈值的点

      //检查过滤后的消息条数，如果消息属性打印0，则表示聚类还没有完成
      notStop = messages.collect.size>0

      if(notStop) {
        //根据Vertexid关联，构造新的图
        //subgraph = subgraph.outerJoinVertices(messages)((vid,bdata, optDeg)=> optDeg.get._1)

        subgraph = subgraph.outerJoinVertices(messages)((vid,vAttr,outerAttr)=>
          if(vAttr != None)
            vAttr
          else if(outerAttr!=None)
            Option(outerAttr.get._1.get)
          else
            None)
      }

      interNum=interNum+1
    }

    subgraph
  }
}

