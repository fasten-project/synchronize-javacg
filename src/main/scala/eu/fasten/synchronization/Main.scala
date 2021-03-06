package eu.fasten.synchronization

import java.util.Properties

import com.typesafe.scalalogging.Logger
import eu.fasten.synchronization.util.{
  SimpleKafkaDeserializationSchema,
  SimpleKafkaSerializationSchema
}
import org.apache.flink.streaming.api.scala.{
  DataStream,
  OutputTag,
  StreamExecutionEnvironment
}
import java.time.Duration
import java.util.concurrent.TimeUnit

import eu.fasten.synchronization.operators.{
  KeyDifferentTopics,
  SynchronizeTopics
}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.connectors.kafka.{
  FlinkKafkaConsumer,
  FlinkKafkaProducer
}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import scopt.OParser

import scala.collection.JavaConversions._

/** Case class to store config variables.
  *
  * @param brokers the Kafka brokers to connect to.
  * @param topicOne the first topic to read from.
  * @param topicTwo the second topic to read from.
  * @param outputTopic the output topic to emit to.
  * @param topicOneKeys the keys to join on for topic one.
  * @param topicTwoKeys the keys to join on for topic two.
  * @param windowTime the amount of time elements are stored in state.
  */
case class Config(brokers: Seq[String] = Seq(),
                  topicOne: String = "",
                  topicTwo: String = "",
                  outputTopic: String = "",
                  topicOneKeys: Seq[String] = Seq(),
                  topicTwoKeys: Seq[String] = Seq(),
                  windowTime: Long = -1,
                  delayTopic: String = "",
                  production: Boolean = false,
                  maxRecords: Int = -1,
                  topicPrefix: String = "fasten",
                  parallelism: Int = 1,
                  backendFolder: String = "/mnt/fasten/flink",
                  enableDelay: Boolean = false)

object Main {

  val configBuilder = OParser.builder[Config]
  val configParser = {
    import configBuilder._
    OParser.sequence(
      programName("Main"),
      head("Flink Synchronization Job"),
      opt[Seq[String]]('b', "brokers")
        .required()
        .valueName("<broker1>,<broker2>,...")
        .action((x, c) => c.copy(brokers = x))
        .text("A set of Kafka brokers to connect to."),
      opt[String]("topic_one")
        .required()
        .valueName("<topic>")
        .text("The first Kafka topic to connect to.")
        .action((x, c) => c.copy(topicOne = x)),
      opt[String]("topic_two")
        .required()
        .valueName("<topic>")
        .text("The second Kafka topic to connect to.")
        .action((x, c) => c.copy(topicTwo = x)),
      opt[String]('o', "output_topic")
        .required()
        .valueName("<topic>")
        .text("The output Kafka topic, --topic_prefix will be prepended.")
        .action((x, c) => c.copy(outputTopic = x)),
      opt[String]("topic_prefix")
        .optional()
        .text("Prefix to add for the output topic. E.g. \"fasten\". ")
        .action((x, c) => c.copy(topicPrefix = x)),
      opt[Seq[String]]("topic_one_keys")
        .required()
        .valueName("<key1>,<key2>,...")
        .text("A set of keys used for the first topic. To get nested keys use \".\". ")
        .action((x, c) => c.copy(topicOneKeys = x)),
      opt[Seq[String]]("topic_two_keys")
        .required()
        .valueName("<key1>,<key2>,...")
        .text("A set of keys used for the first topic. To get nested keys use \".\". ")
        .action((x, c) => c.copy(topicTwoKeys = x)),
      opt[Long]('w', "window_time")
        .required()
        .valueName("<seconds>")
        .text(
          "The time before a delay message is sent when a record is unjoined.")
        .action((x, c) => c.copy(windowTime = x)),
      opt[String]("delay_topic")
        .required()
        .valueName("<string>")
        .text("The topic to output delayed messages to.")
        .action((x, c) => c.copy(delayTopic = x)),
      opt[Boolean]("enable_delay")
        .required()
        .valueName("<boolean>")
        .text("Enable or disable delayed messages.")
        .action((x, c) => c.copy(enableDelay = x)),
      opt[Unit]('p', "production")
        .optional()
        .text("Adding this flag will run the Flink job in production (enabling checkpointing, restart strategies etc.)")
        .action((_, c) => c.copy(production = true)),
      opt[Int]("max_records")
        .optional()
        .hidden()
        .text("Terminates the Kafka sources after receiving this amount of records. Used for development.")
        .action((x, c) => c.copy(maxRecords = x)),
      opt[Int]("parallelism")
        .optional()
        .text("The amount of parallel workers for Flink.")
        .action((x, c) => c.copy(parallelism = x)),
      opt[String]("backendFolder")
        .optional()
        .text("Folder to store checkpoint data of Flink.")
        .action((x, c) => c.copy(backendFolder = x)),
    )
  }

  val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

  val logger = Logger("Main")

  def main(args: Array[String]): Unit = {
    // We need to ensure, we have the correct config.
    val loadedConfig = verifyConfig(args)

    if (loadedConfig.isEmpty) {
      System.exit(1)
    } else {
      logger.info(s"Loaded environment: ${loadedConfig}")
    }

    setupEnvironment(loadedConfig.get)

    val mainStream: DataStream[ObjectNode] = streamEnv
      .addSource(setupKafkaConsumer(loadedConfig.get))
      .uid("kafka-consumer")
      .name("Input Kafka Consumer")
      .keyBy(new KeyDifferentTopics(loadedConfig.get))
      .process(new SynchronizeTopics(loadedConfig.get))
      .uid("synchronize-topics")
      .name("Synchronize Topics")

    // SideOutput
    val delayOutputTag = OutputTag[ObjectNode]("delay-output")
    val sideOutputStream = mainStream.getSideOutput(delayOutputTag)

    mainStream
      .addSink(setupKafkaProducer(loadedConfig.get))
      .uid("kakfa-producer-join")
      .name("Output Kafka Producer")
    sideOutputStream
      .addSink(setupKafkaDelayProducer(loadedConfig.get))
      .uid("kafka-producer-delayed")
      .name("Delayed Kafka Producer")
    streamEnv.execute()
  }

  def setupEnvironment(config: Config): Unit = {
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    if (config.production) {
      streamEnv.setParallelism(config.parallelism)
      streamEnv.enableCheckpointing(5000)
      streamEnv.setStateBackend(new RocksDBStateBackend(
        "file://" +
          config.backendFolder + "/" + config.topicOne + "_" + config.topicTwo + "_sync",
        true))
      streamEnv.setRestartStrategy(
        RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE,
                                            Time.of(10, TimeUnit.SECONDS)))
      val cConfig = streamEnv.getCheckpointConfig
      cConfig.enableExternalizedCheckpoints(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    } else {
      streamEnv.getConfig.setAutoWatermarkInterval(500)
      streamEnv.setParallelism(1)
      streamEnv.setMaxParallelism(1)
    }

  }

  def setupKafkaConsumer(c: Config): FlinkKafkaConsumer[ObjectNode] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", c.brokers.mkString(","))
    properties.setProperty("group.id",
                           f"fasten.${c.topicOne}.${c.topicTwo}.sync")
    properties.setProperty("auto.offset.reset", "earliest")
    properties.setProperty("max.partition.fetch.bytes", "50000000")
    properties.setProperty("message.max.bytes", "50000000")

    val maxRecords: Int = c.maxRecords

    val consumer: FlinkKafkaConsumer[ObjectNode] =
      new FlinkKafkaConsumer[ObjectNode](
        List(c.topicOne, c.topicTwo),
        new SimpleKafkaDeserializationSchema(true, maxRecords),
        properties)

    consumer.assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness[ObjectNode](Duration.ofHours(1))
        .withIdleness(
          if (c.production) Duration.ofMinutes(1) else Duration.ofSeconds(1))
    )

    consumer
  }

  def setupKafkaProducer(c: Config): FlinkKafkaProducer[ObjectNode] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", c.brokers.mkString(","))
    properties.setProperty("max.request.size", "50000000")
    properties.setProperty("message.max.bytes", "50000000")

    val producer: FlinkKafkaProducer[ObjectNode] =
      new FlinkKafkaProducer[ObjectNode](
        c.topicPrefix + "." + c.outputTopic + ".out",
        new SimpleKafkaSerializationSchema(
          c.topicPrefix + "." + c.outputTopic + ".out"),
        properties,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
      )

    producer
  }

  def setupKafkaDelayProducer(c: Config): FlinkKafkaProducer[ObjectNode] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", c.brokers.mkString(","))
    properties.setProperty("max.request.size", "50000000")
    properties.setProperty("message.max.bytes", "50000000")

    val producer: FlinkKafkaProducer[ObjectNode] =
      new FlinkKafkaProducer[ObjectNode](
        c.topicPrefix + "." + c.delayTopic + ".out",
        new SimpleKafkaSerializationSchema(
          c.topicPrefix + "." + c.delayTopic + ".out"),
        properties,
        FlinkKafkaProducer.Semantic.NONE
      )

    producer
  }

  def verifyConfig(args: Array[String]): Option[Config] = {
    OParser.parse(configParser, args, Config())
  }

}
