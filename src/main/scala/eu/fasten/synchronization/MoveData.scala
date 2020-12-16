package eu.fasten.synchronization

import java.util.Properties
import java.util.concurrent.TimeUnit

import eu.fasten.synchronization.Main.streamEnv
import eu.fasten.synchronization.util.{SimpleKafkaDeserializationSchema, SimpleKafkaSerializationSchema}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic

object MoveData {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    streamEnv.setParallelism(4)
    streamEnv.enableCheckpointing(5000)
    streamEnv.setStateBackend(new RocksDBStateBackend(
      "file:///mnt/fasten/flink-javasync/move_data",
      true))
    streamEnv.setRestartStrategy(
      RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE,
        Time.of(10, TimeUnit.SECONDS)))
    val cConfig = streamEnv.getCheckpointConfig
    cConfig.enableExternalizedCheckpoints(
      ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    streamEnv
      .addSource(getConsumer())
      .uid("kafka-consumer-move-data")
      .name("Consume data.")
      .map(_.get("value").deepCopy[ObjectNode]())
      .uid("map-move-data")
      .name("Map data.")
      .addSink(getProducer())
      .uid("kafka-producer-move-data")
      .name("Produce data.")

    streamEnv.execute()
  }

  def getConsumer() = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "172.16.45.120:9092,172.16.45.121:9092,172.16.45.122:9092")
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
    properties.setProperty("bootstrap.servers", "172.16.45.120:9092,172.16.45.121:9092,172.16.45.122:9092")
    properties.setProperty("max.partition.fetch.bytes", "50000000")
    properties.setProperty("message.max.bytes", "50000000")
    properties.setProperty("max.request.size", "50000000")
    properties.setProperty("transaction.timeout.ms", "600000")

    new FlinkKafkaProducer[ObjectNode](
      "fasten.MetadataDBJavaExtension.out",
      new SimpleKafkaSerializationSchema("fasten.MetadataDBJavaExtension.out"),
      properties,
      Semantic.EXACTLY_ONCE)
  }
}
