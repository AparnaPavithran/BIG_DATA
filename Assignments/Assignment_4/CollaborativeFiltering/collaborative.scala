// Databricks notebook source exported at Sat, 29 Oct 2016 00:13:44 UTC
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS

import org.apache.spark.sql.SQLContext


case class Rating(userId: Int, movieId: Int, rating: Double)//, timestamp: Long)
def parseRating(str: String): Rating = {
  val fields = str.split(" ")
  //println(s"Root-mean-square error = ")
  assert(fields.size == 3)//4)
  Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)//, fields(3).toLong)
}

//val ratings = spark.read.textFile("/FileStore/tables/1o9qpxps1477686540937/stats_ALL-bf008.csv").map(parseRating).toDF()
//val ratings = spark.read.textFile("/FileStore/tables/fkltaxxu1477688650215/sample_movielens_data.txt").map(parseRating).toDF()
//val sqlContxt = SQLContext.getOrCreate(SparkContext.getOrCreate())
//val ratings = sqlContxt.read.load("/FileStore/tables/wjhgcxjn1477687688405/u.data")

val ratings = spark.read.textFile("/FileStore/tables/qw99uwdg1477699956025/u.data").map(parseRating).toDF()
val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
// Build the recommendation model using ALS on the training data
val als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").setItemCol("movieId").setRatingCol("rating")//.setTimestampCol("timestamp")
val model = als.fit(training)

// Evaluate the model by computing the RMSE on the test data
val predictions = model.transform(test)
predictions.show()
//dropna(predictions).show()
//predictions.foreach(x=> println(x))
val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")
val rmse = evaluator.evaluate(predictions)
println(s"Root-mean-square error = $rmse")


