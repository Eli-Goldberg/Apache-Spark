package dev.structured.streaming

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

// Run `nc -lk 9999` in bash and start typing

object SocketStream {
  
  // Use new SparkSession interface in Spark 2.0
  val spark = SparkSession
    .builder
    .appName("StreamFromSocket")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "home/compit/dev/spark")
    .getOrCreate()
    
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)    
      
  import spark.implicits._
    
  def main(args: Array[String]) {
    
     val lines = spark.readStream
       .format("socket")
       .option("host", "localhost")
       .option("port", 9999)
       .load()
         
     val words = lines
       .as[String]
       .flatMap(line => line.split("\\s+"))
         
     val wordCounts = words
       .groupByKey(_.toLowerCase())
       .count()
       .orderBy($"count(1)" desc)
         
     val query = wordCounts
       .writeStream
       .outputMode("complete")
       .format("console")
       .start
       .awaitTermination()    
  }
}