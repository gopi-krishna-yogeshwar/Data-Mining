
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.graphframes._
import spire.math.Polynomial.x

import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer

object task1 {
  def main(args: Array[String]): Unit={
    val sparkConf = new SparkConf().setAppName("assign-4-task2").setMaster("local[*]").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
    val sc = new SparkContext(sparkConf)
    val sqlSession = SparkSession.builder().config(sparkConf).getOrCreate()
    sc.setLogLevel("ERROR")
    /*
    val threshold = 7
    val input_file_path = "/Users/gopi/Desktop/Assignment4/ub_sample_data.csv"
    //val betweenFilePath = "/Users/gopi/Desktop/Assignment4/scala/between_scala.csv"
    val outputFilePath = "/Users/gopi/Desktop/Assignment4/scala/between_scala.csv"
*/

    //println("Arguments are:")

    //args.foreach(println)
    val threshold = args(0).toInt
    val input_file_path = args(1)
    //val betweenFilePath = args(2)
    val outputFilePath = args(2)


    val initial_rdd = sc.textFile(input_file_path)
    val first = initial_rdd.first()
    //println(first)
    val grouped = initial_rdd.filter(line => line != first).map(_.split(",")).map(line => (line(0), line(1))).groupByKey()
      .map(line => (line._1, line._2.toList.sortBy(x => x).toSet)).collectAsMap()
    val keySet = grouped.keySet
    val combinations = keySet.subsets(2).map(_.toList).toList
    var vertexSet = Set[String]()
    val vetricesList = ListBuffer[(String, String)]()
    val adjacencyList = ListBuffer[(String, String)]()

    for(userIds <- combinations) {
      val set1: Set[String] = grouped(userIds(0))
      val set2: Set[String] = grouped(userIds(1))
      val intersect = set1.intersect(set2)
      if(intersect.size > threshold) {
        adjacencyList += ((userIds(0), userIds(1)))
        adjacencyList += ((userIds(1), userIds(0)))
        if(!vertexSet.contains(userIds(0))){
          vertexSet  += userIds(0)
          vetricesList += ((userIds(0), userIds(0)))
        }
        if(!vertexSet.contains(userIds(1))){
          vertexSet  += userIds(1)
          vetricesList += ((userIds(1), userIds(1)))
        }
      }
    }

    println(vertexSet.size)
    println(adjacencyList.size)
    //println(combinations.toList(2))
    /*var combinations = Array[(String, String)]()
    for(i<- 0 until(keySet.size)) {
      for(j<- i+1 until(keySet.size)){
        combinations :+ (keySet(i), keySet(j))
      }
    }
    var adjacencyList = ListBuffer[(String, String)]()
    var vertexSet = Set[String]()
    var vertexSetDf = Array[(String, String)]()
    println("Hello World!!")
    for(user_ids <- combinations) {
      val set1: Set[String] = grouped(user_ids._1)
      val set2: Set[String] = grouped(user_ids._2)
      if(!vertexSet.contains(user_ids._1) {
        vertexSet += user_ids._1
        vertexSetDf :+ ((user_ids._1, user_ids._1)
      }
      if(!vertexSet.contains(user_ids(1))) {
        vertexSet += user_ids(1)
        vertexSetDf += ((user_ids(1), user_ids(1)))
      }

      val intersect = set1.intersect(set2)
      if (intersect.size >= threshold) {
        adjacencyList += ((user_ids(0), user_ids(1)))
        adjacencyList += ((user_ids(1), user_ids(0)))
      }
    }
*/

    var vertices = sqlSession.createDataFrame(vetricesList).toDF("id", "id1")
    //println("Size of vertices is :", vertexSetDf.size)
    //println("Size of edges is :", adjacencyList.size)
    vertices = vertices.drop("id1")
    val edges =  sqlSession.createDataFrame(adjacencyList).toDF("src", "dst")
    val graph = GraphFrame(vertices, edges)
    val result_graph = graph.labelPropagation.maxIter(5).run()
    val result_rdd = result_graph.rdd.map(line => (line(1), line(0))).groupByKey().map(x => x._2.toList.map(_.toString).sortBy(x => x)).sortBy(x => (x.size, x(0))).collect().toList//.map(line => line._2.toList.sorted(Ordering.String)).collect()

    writeCommunitiesToFile(result_rdd, outputFilePath)

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
}
