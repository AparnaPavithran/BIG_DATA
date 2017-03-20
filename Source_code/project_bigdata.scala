// Databricks notebook source exported at Mon, 28 Nov 2016 05:28:56 UTC
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
//val graph = GraphLoader.edgeListFile(sc, "/FileStore/tables/t45uq2op1478208616424/sample.txt")
//change path of the dataset here below
val graph = GraphLoader.edgeListFile(sc, "/FileStore/tables/q801qdzx1478988957778/twitter_combined__1_-a997e.txt")
val scc = graph.stronglyConnectedComponents(10)
val cc = graph.connectedComponents()
//cc.vertices.take(81306)
//graph.edges.count() //2420766 
//graph.edges.distinct().count()
//graph.vertices.count() //81306
//cc.edges.count() //2420766
//cc.vertices.count()//81306
//scc.vertices.count()//81306
//scc.edges.count() //2420766
//scc.vertices.take(81306)no
/************************ number of clusters  ************************/
val cc_vertices = cc.vertices.map(x=> (x._2))
cc_vertices.distinct().count() //1 - only one cluster 

/************** number of strongly connected components  **************/
/*val scc_vertices = scc.vertices.map(x=> (x._2))
scc_vertices.distinct().count() //12894 */ //12248 for iteration 10

val scc_vertices = scc.vertices.map(x=> (x._2,1))
val scc_cluster = scc_vertices.reduceByKey((a, b) => a + b)
val scc_cluster_1 = scc_cluster.filter(x => (x._2 >= 3)) //(12,68413)
//scc_cluster_1.foreach(x => (println(x._1,x._2)))

//scc_cluster.foreach(x=> println(x))
//println(scc_cluster.distinct().collect().mkString("\n"))

//println(scc_vertices.distinct().collect().mkString("\n"))
//out_2.collect() 
/***************** Total number of strongly connected clusters *************/
val total_count = cc.edges.distinct().count() //80
var dm  = List[(Long,Float)]()
for (x <- scc_cluster_1.collect())
{
  val sub = scc.subgraph(vpred = (id1,id2) => (id2 == x._1)).edges.distinct().count()
  val tightness = sub.toFloat/total_count.toFloat*100
  val Scc_tightness = (x._1,tightness)
  println(x._1,tightness)
  dm :::= List((x._1, tightness))
}

val listMap = dm.toMap

/*********************** Sorted output **************************/
import scala.collection.mutable.ListMap
ListMap(listMap.toSeq.sortBy(_._2):_*)

/***************************END****************************************/
