package org.yoavshafir.streammeetup

import org.apache.log4j.{Logger, Level}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Seconds}

object Main {
  def main(args: Array[String]) {    
    
    val ssc = new StreamingContext("local[*]", "MeetupStreaming", Seconds(1))
    val meetupStream = MeetupDStream(ssc)
    
    args match {
      case Array() =>
        // Disable logging to make messages more clear
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
        meetupStream.print
      case Array(output) => meetupStream.saveAsTextFiles(s"output/$output", "txt")
      case _ => throw new IllegalArgumentException("Expecting at most one argument");
    }
    
    ssc.start()
    ssc.awaitTermination()
  }
}