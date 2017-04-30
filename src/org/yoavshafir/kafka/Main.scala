package org.yoavshafir.kafka

import org.apache.log4j.{Logger, Level}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Seconds}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/*
  Kafka message fields:
  ---------------------
  OBJECTID,Identifier,OccurrenceDate,DayofWeek,OccurrenceMonth,OccurrenceDay,
  OccurrenceYear,OccurrenceHour,CompStatMonth,CompStatDay,CompStatYear,Offense,
  OffenseClassification,Sector,Precinct,Borough,Jurisdiction,XCoordinate,
  YCoordinate,Latitude,Longitude
 	
 	Actual values:
 	--------------
 	1,987726459,1982-09-21 23:20:0,Tuesday,Sep,21,1982,2300,Apr,9,2015,MURDER,FELONY,,079,
	Brooklyn,NYPD,999634,190253,40.688872153,-73.9445290319999
 	
 */

object Main {
  def main(args: Array[String]) {
    val streamingContext = new StreamingContext("local[*]", "KafkaExample", Seconds(5))
 
    // Disable logging to make messages more clear
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    
    
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = List("nyc-felony-test").toSet
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    
    val boroughAndOffense = kafkaStream.map(record => {
      val parts = record.value().split(",");
      (parts(15) + ":" + parts(11), 1)
    })
    val aggregation = boroughAndOffense.reduceByKeyAndWindow(_ + _, _ - _, Seconds(20))
    val sortedResults = aggregation.transform(rdd => rdd.sortBy(x => x._2, false))
    
    sortedResults.print()
     
    streamingContext.checkpoint("home/compit/dev/spark")
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}