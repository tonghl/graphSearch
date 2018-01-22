package mob

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object OperatorFun {
  def transforEdge(edgeString: String): List[Edge[String]] = {
    val edges = edgeString.trim.replaceAll(" +", " ")
    val vertexs = edges.toString.split(" ")
    if (vertexs.length == 2 && vertexs(0) != vertexs(1)) {
      return List(Edge(vertexs(0).toLong, vertexs(1).toLong, ""));
    }
    return List[Edge[String]]()
  }
}

object FriendSearch {
  /**
    * 构建图
    *
    * @param edgeFilePath
    * @return
    */
  def loadGraph(edgeFilePath: String): Graph[String, String] = {
    val conf = new SparkConf().setAppName("graph").setMaster("local")
    val sc = new SparkContext(conf)
    val edgeRDD = sc.textFile(edgeFilePath).flatMap(OperatorFun.transforEdge(_)).distinct()
    val graph: Graph[Long, String] = Graph.fromEdges(edgeRDD, 0, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK)
    return graph.mapVertices((id, att) => (att.toString))
  }
  def loadGraphNoIndex(edgeFilePath:String)={
    val conf = new SparkConf().setAppName("graph").setMaster("local")
    val sc = new SparkContext(conf)
    val edgeRDD = sc.textFile(edgeFilePath)
                    .map(edgeString=>edgeString.split(" "))
                    .filter(array=>array.length==2&&array(0)!=array(1)).cache()
    /*给每个点生成一个唯一的ID(long)*/
    val vertexWithIndedRdd = edgeRDD.repartition(5).flatMap(x=>x).distinct().zipWithUniqueId()
    vertexWithIndedRdd.foreach(println(_))
    val edgeWithIndex = edgeRDD
      .flatMap(edgeTruple=>{edgeTruple.map(node=>(node,edgeTruple))})
      .leftOuterJoin(vertexWithIndedRdd).map(trup=>{
          val edgeTrup = trup._2._1
          if(trup._1.equals(edgeTrup(0))){
            (s"${edgeTrup(0)}${edgeTrup(1)}",(edgeTrup,trup._2._2,null))
          }else if(trup._1.equals(edgeTrup(1))){
            (s"${edgeTrup(0)}${edgeTrup(1)}",(edgeTrup,null,trup._2._2))
          }else{
            (null,null)
          }

        })
      .filter(truple=>truple._1!=null)
      .reduceByKey((a,b)=>{
        val edgeTruple = a._1
        if(a._2==null){
          (null,b._2,a._3)
        }else{
          (null,a._2,b._3)
        }
      }).map(tuple=>(Edge(tuple._2._2.get,tuple._2._3.get,"")))
    Graph(vertexWithIndedRdd.map(truple=>(truple._2,truple._1)),edgeWithIndex)
  }
  def generaVerId(vertex:RDD[String]): Unit ={
    val vertexNum = vertex.count()
    val vertexPartitionNum = vertex.getNumPartitions
    val partitionNum = vertexNum/vertexPartitionNum
    val rangPartition = (0 until vertexPartitionNum).map(partitionId =>(partitionId*partitionNum,(partitionId+1)*partitionNum))
    val vertexIdRdd = vertex.sparkContext.parallelize(rangPartition,rangPartition.size).flatMap(x=>x._1 until x._2)
    vertexIdRdd.zipWithIndex().foreach(println(_))
  }
  /**
    * 正向搜索图
    *
    * @param graph
    * @return
    */
  def reverseSearch(graph: Graph[String, String], index: Int): Graph[String, String] = {
    val vertexRdd = graph.aggregateMessages[String](
      truplet => {
        if (index == 0) {
          truplet.sendToDst(truplet.srcId.toString)
        } else {
          val srcId = truplet.srcId
          if (truplet.srcAttr != null) {
            val path = truplet.srcAttr.split("\\|").map(node => s"$srcId->${node}").addString(new StringBuilder, "|")
            truplet.sendToDst(path.toString)
          }
        }
      },
      (a, b) => {
        if (a == null && b != null) b else if (a != null && b == null) a else if (a == null && b == null) "" else s"${a}|${b}"
      }
    )
    return Graph(vertexRdd, graph.edges)
  }

  /**
    * 反向搜索图
    *
    * @param graph
    * @return
    */
  def directSearch(graph: Graph[String, String], index: Int): Graph[String, String] = {
    val vertexRdd = graph.aggregateMessages[String](
      truplet => {
        if (index == 0) {
          truplet.sendToSrc(truplet.dstId.toString)
        } else {
          val dstId = truplet.dstId
          if (truplet.dstAttr != null) {
            val path = truplet.dstAttr.split("\\|").map(node => s"$dstId->${node}").addString(new StringBuilder, "|")
            truplet.sendToSrc(path.toString)
          }
        }
      },
      (a, b) => {
        if (a == null && b != null) b else if (a != null && b == null) a else if (a == null && b == null) "" else s"${a}|${b}"
      }
    )
    return Graph(vertexRdd, graph.edges)
  }

  /**
    * 一度正反向搜索
    *
    * @param graph
    * @return Graph(id,(正，反））
    */
  def oneDegreeAll(graph: Graph[String, String]): RDD[(VertexId, (String, String))] = {
    //    val directSearchG = directSearch(graph, 0).vertices
    //    val reverseSearchG = reverseSearch(graph, 0).vertices
    //    return directSearchG.join(reverseSearchG)
    val directSearchG = graph.collectNeighborIds(EdgeDirection.Out).mapValues(array => array.addString(new StringBuilder, "|").toString())
    val reverseSearchG = graph.collectNeighborIds(EdgeDirection.In).mapValues(array => array.addString(new StringBuilder, "|").toString())
    return directSearchG.fullOuterJoin(reverseSearchG).mapValues(truple => (truple._1.getOrElse(null), truple._2.getOrElse(null)))

  }

  /**
    * 二度正反向搜索
    *
    * @param graph
    * @return Graph(id,(正正，反反，正反，反正））
    */
  def twoDegreeAll(graph: Graph[String, String]): RDD[(VertexId, (String, String, String, String))] = {
    /**
      * 注释掉的实现方式会重复计算第一阶段,增加计算时长，后期维护尽量不要使用这种方式
      * var dd = searchByPatch("1,1",graph).vertices.mapValues(att => (att,"dd"))
      * var rr = searchByPatch("-1,-1",graph).vertices.mapValues(att => (att,"rr"))
      * var dr = searchByPatch("1,-1",graph).vertices.mapValues(att => (att,"dr"))
      * var rd = searchByPatch("-1,1",graph).vertices.mapValues(att =>(att,"rd"))
      * dd.union(rr).union(dr).union(rd).aggregateByKey(List[(String,String)]())((list,attr)=>list:+attr,(a,b)=>a:::b ).mapValues(list =>{
      * val map = list.map(tt=>(tt._2,tt._1)).toMap
      * (map("dd"),map("rr"),map("dr"),map("rd"))
      * })
      */
    graph.vertices.cache()
    val d = directSearch(graph, 0)
    d.vertices.cache()
    val r = reverseSearch(graph, 0)
    r.vertices.cache()
    val dd = directSearch(d, 1).vertices.mapValues(att => (att, "dd"))
    val rr = reverseSearch(r, 1).vertices.mapValues(att => (att, "rr"))
    val dr = directSearch(r, 1).vertices.mapValues(att => (att, "dr"))
    val rd = reverseSearch(d, 1).vertices.mapValues(att => (att, "rd"))
    return dd.union(rr).union(dr).union(rd).aggregateByKey(List[(String, String)]())((list, attr) => list :+ attr, (a, b) => a ::: b).mapValues(list => {
      val map = list.map(tt => (tt._2, tt._1)).toMap
      (map("dd"), map("rr"), map("dr"), map("rd"))
    })
  }

  /**
    * 索搜图调度
    *
    * @param direct 1：正向搜索，2反向搜索
    * @param index  0/非0
    * @param graph
    * @return
    */
  def searchScheduler(direct: String, index: Int, graph: Graph[String, String]): Graph[String, String] = {
    if ("1".equals(direct)) {
      return directSearch(graph, index)
    } else if ("-1".equals(direct)) {
      return reverseSearch(graph, index)
    } else {
      throw new Exception(s"索引方向 ${direct} 不正确，只接受1/-1")
    }
  }

  /**
    * 路径搜索图
    *
    * @param path 搜索路径，eg:"1,-1"
    * @param graph
    * @return
    */
  def searchByPatch(path: String, graph: Graph[String, String]): Graph[String, String] = {
    /*判定搜索路径正确性*/
    if (!path.matches("-?1[,-?1]*")) {
      throw new Exception("索引路径不正确，eg:'-1,1-1'")
    }
    var graphTemp: Graph[String, String] = graph
    /*路径索搜*/
    val pathArr = path.split(",")
    for (index <- 0 until pathArr.length) {
      graphTemp = searchScheduler(pathArr(index), index, graphTemp)
    }
    return graphTemp
  }

  def main(args: Array[String]): Unit = {
    val edgeFilePath = "D:\\graph.txt"
//    val graph: Graph[String, String] = loadGraph(edgeFilePath)
//    oneDegreeAll(graph).foreach(println(_))
//    twoDegreeAll(graph).foreach(println(_))
//    loadGraphNoIndex(edgeFilePath).edges.foreach(println(_))
    loadGraphNoIndex(edgeFilePath).triplets.foreach(println(_))
  }
}
