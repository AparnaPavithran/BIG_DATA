// Databricks notebook source exported at Sat, 29 Oct 2016 00:29:04 UTC
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

val sqlContxt = SQLContext.getOrCreate(SparkContext.getOrCreate())
//val data = sqlContxt.read.format("libsvm").load("/FileStore/tables/4b5f6hbr1477617954345/fpgrowth.txt")
//val training = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

val data = sc.textFile("/FileStore/tables/k7zpt5x51477700836342/accidents.txt")

val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))

val fpg = new FPGrowth().setMinSupport(0.2).setNumPartitions(10)
val model = fpg.run(transactions)

model.freqItemsets.collect().foreach { itemset =>
  println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
}

val minConfidence = 0.8
model.generateAssociationRules(minConfidence).collect().foreach { rule =>
  println(
    rule.antecedent.mkString("[", ",", "]")
      + " => " + rule.consequent .mkString("[", ",", "]")
      + ", " + rule.confidence)
}
