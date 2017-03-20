// Databricks notebook source exported at Fri, 28 Oct 2016 19:49:25 UTC

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SQLContext

// Load and parse the data file, converting it to a DataFrame.
val sqlContxt = SQLContext.getOrCreate(SparkContext.getOrCreate())
val training = sqlContxt.read.format("libsvm").load("/FileStore/tables/7hnzj9ti1477676000832/mushroom.txt")
// Load training data
//val training = spark.read.format("libsvm").load("/FileStore/tables/4b5f6hbr1477617954345/sample_libsvm_data.txt")

val lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

// Fit the model
val lrModel = lr.fit(training)

// Print the coefficients and intercept for linear regression
println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

// Summarize the model over the training set and print out some metrics
val trainingSummary = lrModel.summary
println(s"numIterations: ${trainingSummary.totalIterations}")
println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
trainingSummary.residuals.show()
println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
println(s"Coefficient	of	Determination r2: ${trainingSummary.r2}")
