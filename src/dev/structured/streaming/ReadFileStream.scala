package dev.structured.streaming

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

// In order to fake a stream , after starting the program go to the data/parts folder
// and move the files from the `initialLocation` folder to the `finalLocation` folder.
// $> mv initialLocation/parts* finalLocaion/

object ReadFileStream {
  
  // Use new SparkSession interface in Spark 2.0
  val spark = SparkSession
    .builder
    .appName("StructuredStreaming")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "home/compit/dev/spark")
    .getOrCreate()
  
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)    
      
  import spark.implicits._
    
  def main(args: Array[String]) {
    
    // Load data
    val text = (spark.readStream
      .option("maxFilesPerTrigger", 1)
      .text("data/parts/finalLocation")
      .as[String])
    
    // Count words    
    val counts = (text.flatMap(line => line.split("\\s+"))
        .groupByKey(_.toLowerCase())
        .count)
    
    // Print counts    
    val query = (counts
      .orderBy($"count(1)" desc)
      .writeStream
      .outputMode("complete")
      .format("console")
      .start) 
    
    // Keep going until we're stopped.
    query.awaitTermination()
      
    spark.stop()
  }
}