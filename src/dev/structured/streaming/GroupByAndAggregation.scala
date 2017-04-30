package dev.structured.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.log4j.{Level, Logger}

object GroupByAndAggregation {
  
  case class Person (name: String, city: String, country: String, age: Option[Int])
  
  // Use new SparkSession interface in Spark 2.0
  val spark = SparkSession
    .builder
    .appName("Spark Application")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "home/compit/dev/spark")
    .getOrCreate()
  
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)    
  
  import spark.implicits._
  
  def main(args: Array[String]) {
    
    // create schema for parsing data
    val caseSchema = ScalaReflection
      .schemaFor[Person]
      .dataType
      .asInstanceOf[StructType]
          
   val peopleStream = spark
     .readStream
     .schema(caseSchema)
     .option("header", true)
     .option("maxFilesPerTrigger", 1)
     .csv("data/people")
     .as[Person]
       
//   peopleStream.groupBy('country)
//       .mean("age")
//       .writeStream
//       .outputMode("complete")
//       .format("console")
//       .start
//       .awaitTermination()
       
   peopleStream.groupBy('city)
     .agg(first("country") as "country", count("age"))
     .writeStream
     .outputMode("complete")
     .format("console")
     .start
     .awaitTermination() 
  }
  
}