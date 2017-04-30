package dev.structured.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.log4j.{Level, Logger}

// Much as with datasets, we can use case class to represent rows of data,
// However, unlike with dataset, we cannot ask the reader to infer the schema.
// Instead, we will use `ScalaReflection` to generate a schema for our case class.

object CaseClass {
  
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
        
      // passing the case class  
      .schemaFor[Person]
      .dataType
      
      // tell it we want a strcut comming out of it
      .asInstanceOf[StructType]
    
    
   // pass the caseSchema to the stream. 
   val peopleStream = spark
     .readStream
     .schema(caseSchema)
     .option("header", true)
     .option("maxFilesPerTrigger", 1)
     .csv("data/people")
     
       // cast to Person
       .as[Person]
    
    peopleStream
     .writeStream
     .outputMode("append")
     .format("console")
     .start
     .awaitTermination() 
  }
  
}