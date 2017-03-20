// Databricks notebook source exported at Fri, 4 Nov 2016 04:10:20 UTC
import org.apache.spark.graphx.GraphLoader

// Load the edges as a graph
val graph = GraphLoader.edgeListFile(sc, "/FileStore/tables/r6u6l53x1477969810114/ca_HepTh-b2974.txt")
//graph.vertices.collect.foreach(println(_))
//graph.edges.collect.foreach(println(_))
//graph.edges.count()//51971
//graph.vertices.count()//9873
val ccGraph = graph.connectedComponents()


///* Don't TOUCH
//Find the nodes with the highest outdegree and find the count of the number of outgoing edges
val outDegMax = graph.outDegrees
val out = outDegMax.map(_._2).max
//val out_1 = outDegMax.map(x => (x._1,x._2 == out))
val out_2 = outDegMax.filter(x => x._2 ==out).map(y => (y._1,y._2))
println(out_2.collect.mkString("\n"))
//out_2.collect.foreach(println(_))
//Don't TOUCH*/



///* Don't TOUCH
//Find	the	nodes	with	the	highest	indegree	and	find	the	count	of	the	number	of	incoming edges
//val outDegMin = graph.inDegrees
val inDegMax = graph.inDegrees
val in = inDegMax.map(_._2).max
val in_1 = outDegMax.map(x => (x._1,x._2 == out))
val in_2 = outDegMax.filter(x => x._2 ==out).map(y => (y._1,y._2))
//println(in_2.collect.mkString("\n"))
out_2.collect.foreach(println(_))
//Don't TOUCH*/



///* Don't TOUCH
//Calculate	PageRank	for	each	of	the	nodes	and	output	the	top	5	nodes	with	the	larges	
//PageRank	values.	You	are	free	to	define	the	threshold	parameter.
//Page Rank Algorithm // Run PageRank
val ranks = graph.pageRank(0.0001).vertices
//ranks.collect.foreach(println(_)) //(11490,0.6973996668435932)
// Join the ranks with the usernames
val users = graph.vertices.map { line => (line._1,line._2) }
//users.collect.foreach(println(_)) //(11490,1)
val ranksByUsername = users.join(ranks).map {
  case (personid, (edgeweight, rank)) => (personid, rank)
}
//val ranksByUsername_2 = ranksByUsername.collect.map(_._2).sorted.take(5) //sorting
// Print the result
//ranksByUsername_2.foreach(println(_))
//println(ranksByUsername.collect().mkString("\n"))
val ranksByUsername_3 = ranksByUsername.sortBy(_._2, false).take(5)
ranksByUsername_3.foreach(println(_))
//Don't TOUCH*/



///* Don't TOUCH
// Find the connected components
val cc = graph.connectedComponents().vertices
//val sss= graph.stronglyConnectedComponents(10)
//println(sss)
//sss.println(x => x._1)
//println(cc.collect().take(10).mkString("\n"))
// Join the connected components with the usernames
val users = graph.vertices.map { line => (line._1,line._2) }
//println(users.collect().take(10).mkString("\n"))
val ccByUsername = users.join(cc).map {
  case (personid, (edgeweight, cc)) => (personid, cc)
}
// Print the result
println(ccByUsername.collect().mkString("\n"))
//Don't TOUCH*/



/* Not working. Throwing an exception. Due to Partition Strategy 
//Run	the	triangle	counts	algorithm
import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}

// Load the edges in canonical order and partition the graph for triangle count
val graph_1 = GraphLoader.edgeListFile(sc, "/FileStore/tables/r6u6l53x1477969810114/ca_HepTh-b2974.txt", true).partitionBy(PartitionStrategy.RandomVertexCut)
// Find the triangle count for each vertex
val triCounts = graph_1.triangleCount().vertices 
// Join the triangle counts with the usernames
val users = graph_1.vertices.map { line => (line._1,line._2) }
val triCountByUsername = users.join(triCounts).map { case (personid, (edgeweight, tc)) => (personid, tc) }
// Print the result
println(triCountByUsername.collect().mkString("\n")) 
*/
