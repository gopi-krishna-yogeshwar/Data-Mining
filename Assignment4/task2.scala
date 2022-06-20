
import org.apache.spark.{SparkConf, SparkContext}
import collection.mutable.{HashMap, ListBuffer}
import scala.math.BigDecimal
import java.io._
import scala.collection.mutable
object task2 {

  def main(args: Array[String]): Unit={
    val sparkConf = new SparkConf().setAppName("assign-4-task2").setMaster("local[*]").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")

/*
    val threshold = 7
    val input_file_path = "/Users/gopi/Desktop/Assignment4/ub_sample_data.csv"
    val betweenFilePath = "/Users/gopi/Desktop/Assignment4/scala/between_scala.csv"
    val outputFilePath = "/Users/gopi/Desktop/Assignment4/scala/output_2_scala.csv"
*/


    println("Arguments are:")
    args.foreach(println)
    val threshold = args(0).toInt
    val input_file_path = args(1)
    val betweenFilePath = args(2)
    val outputFilePath = args(3)

    val initial_rdd = sc.textFile(input_file_path)
    val first = initial_rdd.first()
    println(first)
    val grouped = initial_rdd.filter(line => line != first).map(_.split(",")).map(line => (line(0), line(1))).groupByKey()
      .map(line => (line._1, line._2.toList.sortBy(x => x).toSet)).collectAsMap()
    val keySet = grouped.keySet
    val combinations = keySet.subsets(2).map(_.toList)
    val adjacencyList = new HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String]
    val adjacencyListCopy = new HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String]
    var dataSet = Set[String]()

    for(user_ids <- combinations) {
      val set1: Set[String] = grouped(user_ids(0))
      val set2: Set[String] = grouped(user_ids(1))

      val intersect = set1.intersect(set2)
      if (intersect.size >= threshold) {
        adjacencyList.addBinding(user_ids(0), user_ids(1))
        adjacencyList.addBinding(user_ids(1), user_ids(0))
        adjacencyListCopy.addBinding(user_ids(0), user_ids(1))
        adjacencyListCopy.addBinding(user_ids(1), user_ids(0))
        var key = getKey(user_ids(0),  user_ids(1))
        dataSet += key
      }
    }
    //adjacencyList.foreach(println)
    //println(adjacencyList.size)
    var betweeness = getBetweeness(adjacencyList)
    writeBetweenessToFile(betweeness, betweenFilePath)
    var m = betweeness.size

    var communities = getCommunities(adjacencyList)
    var modularity = getModularity(adjacencyList, communities, m, dataSet)
    var totalEdges = betweeness.size

    while(totalEdges > 0) {
      var largeEdgeValue = betweeness(0)._2
      var largeEdge = betweeness(0)._1

      for( edge <- betweeness) {
        val key = edge._1
        if(edge._2 == largeEdgeValue) {
          var v1 = getVertexFromKey(key)(0)
          var v2 = getVertexFromKey(key)(1)
          adjacencyListCopy(v1).remove(v2)
          adjacencyListCopy(v2).remove(v1)
          totalEdges -= 1
        }
      }

      var modifiedCommunities = getCommunities(adjacencyListCopy)
      var modifiedModularity = getModularity(adjacencyList, modifiedCommunities, m, dataSet)

      if(modifiedModularity >= modularity) {
        communities = modifiedCommunities
        modularity = modifiedModularity
      }
      betweeness = getBetweeness(adjacencyListCopy)
    }

    communities = communities.sortWith((a,b) => a.size < b.size || (a.size == b.size && a(0) < b(0)))
    writeCommunitiesToFile(communities, outputFilePath)
    //communities.foreach(println)
    //println(communities.size)
  }


  def  getKey(s1: String, s2:String) : String={
    var key = ""
    if(s1 < s2) {
      key = s1 + ":" + s2
    } else {
      key = s2 + ":" + s1
    }
    return key
  }

  def getVertexFromKey(key : String): Array[String] ={
    return key.split(":")

  }
  def getBetweeness(adjacencyList: HashMap[String, mutable.Set[String]]) : List[(String, Double)]={
    val vertices = adjacencyList.keySet.toList
    var results = new HashMap[String, Double].withDefaultValue(0.0)
    val counts = Array.fill(20){0}
    //print(counts)
    for(vertex <- vertices){
      val neighbours = mutable.Queue[String]()
      var nodesInQueue = Set[String]()
      neighbours.enqueue(vertex)
      nodesInQueue += vertex

      val childParentMap = new HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String]
      val shortestPathCount = new HashMap[String, Double].withDefaultValue(0.0)
      shortestPathCount(vertex) = 1.0
      val levelsMap = new HashMap[String, Int].withDefaultValue(0)
      levelsMap(vertex) = 0

      while(!neighbours.isEmpty) {
        val currentNode = neighbours.dequeue()
        //nodesInQueue -= currentNode
        val nextLevelNodes = adjacencyList(currentNode)
        for(neighbour <- nextLevelNodes){
          if(!levelsMap.contains(neighbour)) {
            levelsMap(neighbour) = levelsMap(currentNode) + 1
            shortestPathCount(neighbour) = 1.0

            if (!nodesInQueue.contains(neighbour)){
              neighbours.enqueue(neighbour)
              nodesInQueue += neighbour
            }
            childParentMap.addBinding(neighbour, currentNode)
          } else {
            if(levelsMap(neighbour) ==  levelsMap(currentNode) + 1) {
              childParentMap.addBinding(neighbour, currentNode)
              shortestPathCount(neighbour) = shortestPathCount(neighbour) + 1.0
            }
          }
        }
      }

      var set = levelsMap.values.toSet
      //println(set.size)
      counts(set.size) = counts(set.size) + 1

      //println(nodesInQueue.size)
      var nodeScore = new HashMap[String, Double].withDefaultValue(1.0)
      var betweeness = new HashMap[String, Double]

      val sortedVertices = levelsMap.toSeq.sortWith(_._2 > _._2).toList
      for(sortedVertex <- sortedVertices){
        var node = sortedVertex._1
        if(node != vertex){
          val parents = childParentMap(node)
          var share = 0.0
          for(parent <- parents){
            share = share + shortestPathCount(parent)
          }
          //println(share)
          for(parent <- parents) {
            val key = Array(parent, node).toSeq.sortBy(x => x).toList
            val key1 = key(0) + ":" + key(1)
            betweeness(key1) = (nodeScore(node) * shortestPathCount(parent)) / share
            nodeScore(parent) = nodeScore(parent) + betweeness(key1)
          }
        }
      }
      //betweeness.foreach(println)

      for(edge <- betweeness.keySet) {
        results(edge) = results(edge) + (betweeness(edge) / 2.0)
      }

    }
    val finalResults = results.toSeq.sortWith((a,b) => a._2 > b._2).toList
    //println(finalResults(0))
    //println(finalResults.size)
    //println(finalResults)
    //counts.foreach(println)
    return finalResults
  }

  def getCommunities(adjacencyList: HashMap[String, mutable.Set[String]]) : List[List[String]]={
    var communities = List[List[String]]()
    var vertices = adjacencyList.keySet
    //println(vertices.size)
    while(vertices.size > 0){
      var verticesList = vertices.toList
      var firstNode = verticesList(0)
      var visitedNodes = Set[String]()

      var neighbours = mutable.Queue[String]()
      var nodesInQueue = Set[String]()
      neighbours.enqueue(firstNode)
      nodesInQueue += firstNode

      while(!neighbours.isEmpty) {
        var vertex = neighbours.dequeue()
        visitedNodes += vertex

        var adjacentNodes = adjacencyList(vertex)
        for(adjacentNode <- adjacentNodes) {
          if (!visitedNodes.contains(adjacentNode) && !nodesInQueue.contains(adjacentNode)) {
            neighbours.enqueue(adjacentNode)
            nodesInQueue += adjacentNode
          }
        }
      }

      var community = visitedNodes.toSeq.sortWith((a,b) => a < b).toList
      communities = communities :+ community
      vertices = vertices.diff(visitedNodes)
    }
    return communities
  }

  def getModularity(adjacencyList: HashMap[String, mutable.Set[String]], communities : List[List[String]], m : Int, dataSet : Set[String]): Double = {

    var result = 0.0
    for(community <- communities) {
      for(node1 <- community){
        for(node2 <- community) {
          var key  = getKey(node1, node2)

          var length1 = adjacencyList(node1).size
          var length2 = adjacencyList(node2).size
          var value = 0
          if(dataSet.contains(key)) {
            value = 1
          }

          val communityModularity = value - ((length1 * length2) / (2.0 * m))
          //println(communityModularity)
          result += communityModularity
        }
      }
    }
    return result / (2.0 * m)
  }

  def writeCommunitiesToFile(results: List[List[String]], outputFilePath: String) : Unit={
    val pw = new PrintWriter(new File(outputFilePath))
    for(community <- results) {
      var string = ""
      for(i <- 0 until(community.size)) {
        //println("community is:",community(i))
        if(i != community.size-1) {
          string = string + "'" + community(i) + "', "
        } else {
          string = string + "'" + community(i) + "'"
        }
      }
      //println("String is :", string)
      pw.write(string)
      pw.write("\n")
    }
    pw.close()
  }

  def writeBetweenessToFile(resultsUnsorted: List[(String, Double)], outputFilePath: String) : Unit={
    //var results = resultsUnsorted.toSeq.sortWith((a,b) => a._2 > b._2 || (a._2 == b._2 && a._1(0) < b._1(0)) || (a._2 == b._2 && a._1(0) == b._1(0) && a._1(1) < b._1(1))).toList
    var results = getBetweenessStrings(resultsUnsorted)
    val pw = new PrintWriter(new File(outputFilePath))
    for(community <- results) {
      var string = ""
      var b = BigDecimal(community._3).setScale(5, BigDecimal.RoundingMode.HALF_UP).toString
      b = stripZero(b)
      string = "('" + community._1 + "', '" + community._2 + "')," + b
      pw.write(string)
      pw.write("\n")
    }
    pw.close()
  }

  def getBetweenessStrings(results : List[(String, Double)]) : List[(String, String, Double)]={
    var finalData = ListBuffer[(String, String, Double)]()
    finalData.foreach(println)
    for(result <- results) {
      var str = result._1.split(":")
      finalData += ((str(0), str(1), result._2))
    }
    //val x = finalData.sortWith((a,b) => (a._3 > b._3) || (a._3 == b._3 && a._1 < b._1)).toList
    var x = finalData.sortBy(data => (-data._3, data._1)).toList
    return x
  }

  def stripZero(b : String) : String={
    val index = b.indexOf(".")
    var finalString = ""
    var substring = b.substring(index+1, b.size)
    var size = substring.size-1
    var trailingCount = 0
    if(substring(size) == '0') {
      trailingCount+=1
    }
    if(substring(size-1) == '0' && trailingCount ==1) {
      trailingCount += 1
    }
    if(substring(size-2) == '0' && trailingCount ==2) {
      trailingCount += 1
    }
    if(substring(size-3) == '0' && trailingCount ==3) {
      trailingCount += 1
    }
    if(substring(size-4) == '0' && trailingCount ==4) {
      trailingCount += 1
    }
    if(trailingCount== 1) {
      finalString = b.substring(0, b.size-1)
    }

    if(trailingCount == 2) {
      finalString = b.substring(0, b.size-2)
    }
    if(trailingCount == 3) {
      finalString = b.substring(0, b.size-3)
    }
    if(trailingCount == 4) {
      finalString = b.substring(0, b.size-4)
    }
    if(trailingCount == 5) {
      finalString = b.substring(0, b.size-4)
    }
    if(finalString == "") {
      finalString = b
    }
    return finalString
  }

}
