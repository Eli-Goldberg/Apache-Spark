package dev.structured.streaming

import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

// Wordcount program usting Dataset.

object WordcountDataset {
  
  // Create the Spark Session and the spark context				
	val spark = SparkSession
  	.builder
  	.appName(getClass.getSimpleName)
  	.master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "home/compit/dev/spark")
  	.getOrCreate()
	
	val sc = spark.sparkContext
	
	import spark.implicits._
	
  def main(args: Array[String]) {
    
	  // Load data. returns a Dataset
	  val text = spark.read.text("data/lorem.txt").as[String]
	  
	  // Load data. returns a Dataset
	  val lines = text.flatMap(line => line.split("\\s+"))
	  
	  // return KeyValueGroupedDataset
	  val group = lines.groupByKey(_.toLowerCase)
	  
	  // Load data. returns a Dataset
	  val counts = group.count
	  println(counts.getClass)
	  
	  // Dispaly most common
	  counts.orderBy($"count(1)" desc).show
  }  
}