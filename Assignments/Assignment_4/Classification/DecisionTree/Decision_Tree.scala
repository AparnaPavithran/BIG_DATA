// Databricks notebook source exported at Fri, 28 Oct 2016 04:15:30 UTC
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.SQLContext
val sqlContxt = SQLContext.getOrCreate(SparkContext.getOrCreate())
val data = sqlContxt.read.format("libsvm").load("/FileStore/tables/xxmsnec01477626466446/breast_cancer.txt")
val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data)
val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)
val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))
val dt = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")
val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))
val model = pipeline.fit(trainingData)
val predictions = model.transform(testData)
val calculate = predictions.filter("predictedLabel = label ")
val accuracy = calculate.count().toDouble/testData.count().toDouble
println("Accuracy = " + accuracy)
predictions.select("predictedLabel", "label").show(5)
val evaluator_1 = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("f1")
val f1 = evaluator_1.evaluate(predictions)
println("f1-measure = " + f1)
val evaluator_2 = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("weightedPrecision")
val weightedPrecision = evaluator_2.evaluate(predictions)
println("Weighted Precision = " + weightedPrecision)
val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
println("Learned classification tree model:\n" + treeModel.toDebugString)

