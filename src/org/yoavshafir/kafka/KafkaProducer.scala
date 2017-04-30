package org.yoavshafir.kafka

import scala.io.Source
import java.util.concurrent.Future
import org.apache.kafka.clients.producer.RecordMetadata

object KafkaProducer {
  
  def main(args: Array[String]) {
    val topic = "nyc-felony-test"
    println(s"Connecting to $topic")
  
    import org.apache.kafka.clients.producer.KafkaProducer
    
    val props = new java.util.Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("client.id", "KafkaProducer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  
    val producer = new KafkaProducer[Integer, String](props)
  
    import org.apache.kafka.clients.producer.ProducerRecord
    
    for (line <- Source.fromFile("data/Felony.csv").getLines) {
      val record = new ProducerRecord[Integer, String](topic, 1, line.toString)
      val metaF: Future[RecordMetadata] = producer.send(record)
      val meta = metaF.get() // blocking!
//      val msgLog =
//        s"""
//           |offset    = ${meta.offset()}
//           |partition = ${meta.partition()}
//           |topic     = ${meta.topic()}
//         """.stripMargin
//      println(msgLog)
       
      Thread.sleep(250)
    }
  }
}