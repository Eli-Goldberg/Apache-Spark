package dev.structured.streaming

import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

object JoinDataset {
   
    // Create the Spark Session and the spark context				
  	val spark = SparkSession
			.builder
			.appName(getClass.getSimpleName)
			.master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "home/compit/dev/spark")
			.getOrCreate()
  	
  	import spark.implicits._
  	
  	case class User(id: Int, name: String, email: String, country: String)
  	case class Transaction(userid: Int, product: String, cost: Double)
  	
  	def main(args: Array[String]) {
  	  
    	val users = spark.read
    	  .option("inferSchema", "true")
    	  .option("header", "true")
    	  .csv("data/users.csv")
    	  .as[User]
    	
    	val transactions = spark.read
    	  .option("inferSchema", "true")
    	  .option("header", "true")
    	  .csv("data/transactions.csv")
    	  .as[Transaction]
    	
    	users.join(transactions, users.col("id") === transactions.col("userid"))
    	  .groupBy($"name")
    	  .sum("cost")
    	  .show
    }	   	  
}