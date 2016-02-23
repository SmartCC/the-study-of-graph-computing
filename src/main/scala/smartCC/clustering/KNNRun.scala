package smartCC.clustering

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{Graph,Edge}
import org.apache.spark.graphx.util.GraphGenerators 
import org.apache.spark.{SparkConf, SparkContext}


object KNNRun {
  def main(args : Array[String]) {
    if(args.length != 5){
      System.err.println("Useage : KNNRun <inputDir> k-points N-neighbor iter-num min-distance")
      exit(1)
    }

    val sparkConf = new SparkConf()
      .setAppName("KNNRun")
      .setMaster("yarn-client")
    val sc = new SparkContext(sparkConf)

    //创建随机图
    val graph: Graph[Long, Double] = 
            GraphGenerators.logNormalGraph(sc, numVertices = 100).mapEdges(e => e.attr.toDouble)

    //测试50个中心点，10个邻近值，迭代50次，阈值为0.0005
    //KNN.knn(graph,50,10,50,0.005)
    KNN.knn(graph,args(1).toInt,args(2).toInt,args(3).toInt,args(4).toDouble).
      vertices.
      join(id2Names).
      map{case (tid,(cid,tagName)) => (cid,tid+":"+tagName)}.
      groupByKey().
      map(t=>{if(t._1==None) "未分类|"+t._2.mkString(",") else t._1.get+"|"+t._2.mkString(",")}).
      foreach(println)
  }  
}
