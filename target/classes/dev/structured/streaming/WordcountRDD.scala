package dev.structured.streaming

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

// Simple wordcount program in a batch approach.

object WordcountRDD {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Wordcount")
      .setMaster("local[*]")
    
    // CHAINING FUNCTIONS CODING STYLE
    // -------------------------------
      
    val sc = new SparkContext(conf)
    
    // Load lines, will create an RDD which holds all the lines.
    val lines = sc.textFile("data/lorem.txt")
    
    val counts = lines.flatMap(line => line.split("\\s+"))
      .map(word => (word.toLowerCase, 1))
      .reduceByKey(_+_)
    
    counts.sortBy(_._2, ascending=false)
      .take(20)
      .foreach(println)
    
    // SINGLE LINE CODING STYLE
    // ------------------------
    
    /*  
    val lines = sc.textFile("data/lorem.txt")  
    
    // Debuging
    lines.foreach(println)
    
    val words = (lines.flatMap(line => line.split("\\s+")))
    
    // Debuging
    words.foreach(println)
    
    val pairs = words.map(word => (word.toLowerCase(), 1))
    
    // Debuging
    pairs.foreach(println)
    
    val counts = pairs.reduceByKey(_ + _)
    
    // Debuging
    counts.foreach(println)
    
    counts.sortBy(_._2, ascending=false)
      .take(20)
      .foreach(println)
    * 
    */
  }  
}