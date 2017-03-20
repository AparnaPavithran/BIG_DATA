// Databricks notebook source exported at Fri, 28 Oct 2016 17:19:36 UTC
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SQLContext

val sqlContxt = SQLContext.getOrCreate(SparkContext.getOrCreate())
val data = sqlContxt.read.format("libsvm").load("/FileStore/tables/vpe9lovp1477674893357/coloncancer.txt")
//val training = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
//val data = spark.read.format("libsvm").load("/FileStore/tables/9hylir121477673147463/breast_cancer_copy-182f1.txt")
val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

// Fit the model
val lrModel = lr.fit(data)

// Print the coefficients and intercept for logistic regression
println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
