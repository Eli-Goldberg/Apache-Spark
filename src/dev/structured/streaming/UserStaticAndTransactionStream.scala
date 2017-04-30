package dev.structured.streaming

import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._

object UserStaticAndTransactionStream {
  
    // Create the Spark Session and the spark context				
  	val spark = SparkSession
  		.builder
			.appName(getClass.getSimpleName)
			.master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "home/compit/dev/spark")
			.getOrCreate()
  	
  	val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  
  	import spark.implicits._
  	
  	case class User(id: Int, name: String, email: String, country: String)
  	case class Transaction(userid: Int, product: String, cost: Double)
  	
  	def main(args: Array[String]) {
  	  
  	  // This is a static Dataset.
  	  // We use the `read` method and not the `readStream` method.
  	  // We dont need to provide a schema.
  	  // We can simply infer it.
    	val users = spark.read
    	  .option("inferSchema", "true")
    	  .option("header", "true")
    	  .csv("data/users.csv")
    	  .as[User]
    	
    	val transactionSchema = ScalaReflection
    	  .schemaFor[Transaction]
    	  .dataType
    	  .asInstanceOf[StructType]
    	
    	val transactionStream = spark.readStream
    	  .schema(transactionSchema)
    	  .option("header", true)
    	  .option("maxFilesPerTrigger", 1)
    	  .csv("data/transactions/*.csv")
    	  .as[Transaction]
    	
    	// Join transaction stream with user dataset
      val spendingByCountry = transactionStream
        .join(users, users("id") === transactionStream("userid"))
        .groupBy($"country")
        .agg(sum($"cost")) as "spending"
        
      // Print result
      spendingByCountry
        .writeStream
        .outputMode("complete")
        .format("console")
        .start
        .awaitTermination() 
    }	 
}