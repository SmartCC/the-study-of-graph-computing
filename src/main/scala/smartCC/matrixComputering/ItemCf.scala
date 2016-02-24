package smartCC.matrixComputering

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{Graph, Edge}  
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Row

import scala.math.{pow, sqrt}

/*
 算法原理：
   1、矩阵中的两列向量向量相乘，就是对应值相乘，相乘的两个元素必须具有相同的索引，索引不同乘机为0
   2、通过使用关联关系，item1->users 与item2->users的节点取交集，即需要计算的数据
   3、相应连接的边就是矩阵(i,j)的数值
 程序设计原理：
   1、item向user节点发送本身的id数据,user节点存储所有接收到的itemId
   2、user节点把接收到的数据反向发送给所有关联的item节点，本程序使用欧几里得距离，发送的信息为：发送边的值×原信息自带的值，
   3、item节点接收到数据之后，就可以知道该user节点与哪些关联
   4、item接收到的消息按item分类，每个分类就是两者想关联的情况
   5、归一化
 */

object ItemCf{
  //list合并函数，模仿clojure的mergeWith
  def mergeWith[KT, VT](f: (VT, VT) => (VT), l1: List[(KT, VT)], l2: List[(KT, VT)]): List[(KT, VT)] = {
    val l = l1 ::: l2
    l.groupBy(_._1).map(m => (m._1, m._2.map(_._2).reduce(f))).toList
  }

  //输入数据为(userId,itemId,score)
  def computerSimilarMatrix(releations : RDD[(Long,Long,Double)]) : RDD[((Long,Long),Double)] ={
    //tag和name的对应关系
    val vertices = releations.flatMap{case (userId,itemId,score)=>Array((userId,"userId"),(itemId,"itemId"))}.distinct
    //user与item的定义关系
    val edges = releations.distinct.map{case (userId,itemId,score)=>Edge(userId,itemId,score)}

    //构建图
    /*节点数据样例为：
     (7729932,user)
     (17,tag)
     (37701526,user)
     (94,tag)
     ... ...
     */
    val graph = Graph(vertices, edges)

    //获取读个数，开方，作为归一化的分母
    //数据样例为[(37701526,1.0)(94,2.0)(17,1.7320508075688772),...],前一个为userId或itemId，后一个为分数
    val verticesValue = graph.aggregateMessages[Double](
      ctx => {
        //此处不用去重，因为前边已经去重，可以确定每两个点之间只有条边（且是单向的）
        //其他情况一定注意去重
        ctx.sendToDst(pow(ctx.attr,2))
        ctx.sendToSrc(pow(ctx.attr,2))
      },
        (msg1,msg2) => msg1+msg2
    ).map{case (id,s) => (id, sqrt(s))}

    //计算商品相似度，用户相似度类似
    //数据样例[(7729932,List((17,0.3))),(37701526,List((94,0.9))),(82057467,List((17,0.4))),...],前面为id号，后边为收到的id号以及对应的边的分数
    val step1 = graph.aggregateMessages[List[(Long,Double)]](
      ctx => {
        if (ctx.dstAttr == "item") ctx.sendToSrc(List[(Long,Double)]((ctx.dstId,ctx.attr)))},
      (a, b) => a ::: b).
      map{case (itemId,msgs)=>(itemId,msgs.toSet.toList)}

    //同原图关联，更新节点的数据
    /*节点数据样例：
    (37701526,Some(List((94,0.9)）)))
    (94,None)
    (17,None)
    ... ...
    */

    val graph1 = graph.outerJoinVertices(step1)((vid,bdata, optDeg) => optDeg)

    //数据样例[(94,List((94,9.4))),(17,List((17,3.5)))]
    //都是属性节点的数据，后边的是其他节点和跟当前节点的数值乘机之和
    val step2 = graph1.aggregateMessages[List[(Long, Double)]](
      ctx => {
        if (ctx.srcAttr != None)
          ctx.sendToDst(
            ctx.srcAttr.get.
              filter {case(id,_) => id!=ctx.dstId}. //过滤掉与目标节点相同节点id
              map{case (id,score) => (id,score*ctx.attr)})}, //发送边的值×原信息自带的值
      (msg1, msg2) => mergeWith((x: Double, y: Double) => x + y, msg1, msg2))

    val similarMatrix =
      step2.join(verticesValue). //关联，为第一次归一化连接数据
        flatMap{ case(id,(idScoreList,idScores)) =>
          (idScoreList.map(t => (t._1, t._2 /idScores)).map(t => (t._1, (id, t._2))))}. //第一次归一化，并使用flatMap展开
        join(verticesValue). //关联，为第二次归一化连接数据
        map{case (itemId1,((itemId2,score),id1Scores))=> ((itemId1,itemId2),score/id1Scores)}.   //第二次归一化 (94,(35,0.37))
        sortBy(t => t._2, false)  //按降序排列

    similarMatrix
  }
}
