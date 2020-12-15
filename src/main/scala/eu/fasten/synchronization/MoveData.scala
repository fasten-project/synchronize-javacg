package eu.fasten.synchronization

import java.util.Properties

import eu.fasten.synchronization.util.{
  SimpleKafkaDeserializationSchema,
  SimpleKafkaSerializationSchema
}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{
  FlinkKafkaConsumer,
  FlinkKafkaProducer
}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic

object MoveData {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    streamEnv
      .addSource(getConsumer())
      .map(_.get("value"))
      .print()
  }

  def getConsumer() = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "")
    properties.setProperty("group.id", f"move_data_group")
    properties.setProperty("auto.offset.reset", "earliest")
    properties.setProperty("max.partition.fetch.bytes", "50000000")
    properties.setProperty("message.max.bytes", "50000000")

    new FlinkKafkaConsumer[ObjectNode](
      "fasten.MetadataDBExtension.out",
      new SimpleKafkaDeserializationSchema(false, -1),
      properties)
  }

  def getProducer() = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "")
    properties.setProperty("group.id", f"move_data_group")
    properties.setProperty("auto.offset.reset", "earliest")
    properties.setProperty("max.partition.fetch.bytes", "50000000")
    properties.setProperty("message.max.bytes", "50000000")

    new FlinkKafkaProducer[ObjectNode](
      "fasten.MetadataDBJavaExtension.out",
      new SimpleKafkaSerializationSchema("fasten.MetadataDBJavaExtension.out"),
      properties,
      Semantic.NONE)
  }
}
