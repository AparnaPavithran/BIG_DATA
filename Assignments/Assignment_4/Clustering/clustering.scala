// Databricks notebook source exported at Fri, 28 Oct 2016 17:35:31 UTC
import org.apache.spark.ml.clustering.KMeans

// Loads data.
import org.apache.spark.sql.SQLContext
val sqlContxt = SQLContext.getOrCreate(SparkContext.getOrCreate())
val dataset = sqlContxt.read.format("libsvm").load("/FileStore/tables/7hnzj9ti1477676000832/mushroom.txt")
//val dataset = spark.read.format("libsvm").load("/FileStore/tables/4b5f6hbr1477617954345/sample_libsvm_data.txt")

// Trains a k-means model.
val kmeans = new KMeans().setK(2).setSeed(1L)
val model = kmeans.fit(dataset)

// Evaluate clustering by computing Within Set Sum of Squared Errors.
val WSSSE = model.computeCost(dataset)
println(s"Within Set Sum of Squared Errors = $WSSSE")

// Shows the result.
println("Cluster Centers: ")
model.clusterCenters.foreach(println)
