package eu.fasten.synchronization.util

import java.lang
import java.nio.charset.StandardCharsets

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

class SimpleKafkaSerializationSchema(topic: String)
    extends KafkaSerializationSchema[ObjectNode] {

  override def serialize(
      element: ObjectNode,
      timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    new ProducerRecord[Array[Byte], Array[Byte]](
      topic,
      element.toString.getBytes(StandardCharsets.UTF_8))
  }

}
