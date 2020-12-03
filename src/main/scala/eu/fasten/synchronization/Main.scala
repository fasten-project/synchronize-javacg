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

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.scala._
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.connectors.kafka.{
  FlinkKafkaConsumer,
  FlinkKafkaProducer
}
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema

import scala.collection.JavaConversions._

/** Case class to store environment variables.
  *
  * @param brokers the Kafka brokers to connect to.
  * @param topicOne the first topic to read from.
  * @param topicTwo the second topic to read from.
  * @param outputTopic the output topic to emit to.
  * @param topicOneKeys the keys to join on for topic one.
  * @param topicTwoKeys the keys to join on for topic two.
  * @param windowTime the amount of time elements are stored in state.
  */
case class Environment(brokers: List[String],
                       topicOne: String,
                       topicTwo: String,
                       outputTopic: String,
                       topicOneKeys: List[String],
                       topicTwoKeys: List[String],
                       windowTime: Long)

object Main {

  val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

  val logger = Logger("Main")

  val topicPrefix = "fasten"

  def main(args: Array[String]): Unit = {
    // We need to ensure, we have the correct environment variables.
    val loadedEnv = verifyEnvironment()
    if (loadedEnv.isEmpty) {
      System.exit(1)
    } else {
      logger.info(s"Loaded environment: ${loadedEnv}")
    }

    //streamEnv.enableCheckpointing(1000)

    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.getConfig.setAutoWatermarkInterval(500)
    streamEnv.setParallelism(1)
    streamEnv.setMaxParallelism(1)

    val mainStream: DataStream[ObjectNode] = streamEnv
      .addSource(setupKafkaConsumer(loadedEnv.get))
      .keyBy(new KeyDifferentTopics(loadedEnv.get))
      .process(new SynchronizeTopics(loadedEnv.get))

    // SideOutput
    val errOutputTag = OutputTag[ObjectNode]("err-output")
    val sideOutputStream = mainStream.getSideOutput(errOutputTag)

    mainStream.addSink(setupKafkaProducer(loadedEnv.get))
    sideOutputStream.addSink(setupKafkaErrProducer(loadedEnv.get))

    streamEnv.execute()
  }

  def setupKafkaConsumer(
      environment: Environment): FlinkKafkaConsumer[ObjectNode] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers",
                           environment.brokers.mkString(","))
    properties.setProperty(
      "group.id",
      f"fasten.${environment.topicOne}.${environment.topicTwo}.sync")
    properties.setProperty("auto.offset.reset", "earliest")

    val maxRecords: Int = sys.env.get("MAX_RECORDS").map(_.toInt).getOrElse(-1)

    val consumer: FlinkKafkaConsumer[ObjectNode] =
      new FlinkKafkaConsumer[ObjectNode](
        List(environment.topicOne, environment.topicTwo),
        new SimpleKafkaDeserializationSchema(true, maxRecords),
        properties)

    consumer.assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness[ObjectNode](Duration.ofHours(1))
        .withIdleness(Duration.ofMinutes(1))
    )

    consumer
  }

  def setupKafkaProducer(
      environment: Environment): FlinkKafkaProducer[ObjectNode] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers",
                           environment.brokers.mkString(","))

    val producer: FlinkKafkaProducer[ObjectNode] =
      new FlinkKafkaProducer[ObjectNode](
        topicPrefix + "." + environment.outputTopic + ".out",
        new SimpleKafkaSerializationSchema(environment.outputTopic),
        properties,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
      )

    producer
  }

  def setupKafkaErrProducer(
      environment: Environment): FlinkKafkaProducer[ObjectNode] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers",
                           environment.brokers.mkString(","))

    val producer: FlinkKafkaProducer[ObjectNode] =
      new FlinkKafkaProducer[ObjectNode](
        topicPrefix + "." + environment.outputTopic + ".err",
        new SimpleKafkaSerializationSchema(environment.outputTopic),
        properties,
        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
      )

    producer
  }

  def verifyEnvironment(): Option[Environment] = {

    val inputEnv =
      List("KAFKA_BROKER",
           "INPUT_TOPIC_ONE",
           "INPUT_TOPIC_TWO",
           "OUTPUT_TOPIC",
           "TOPIC_ONE_KEYS",
           "TOPIC_TWO_KEYS",
           "WINDOW_TIME")
    val envMapped = inputEnv.map(x => (x, sys.env.get(x)))
    val filterMap = envMapped.filter(_._2.isEmpty)

    filterMap.foreach { x =>
      logger.error(s"Environment variable ${x._1} could not be found.")
    }

    if (filterMap.size > 0) {
      return None
    }

    val environmentFinal = envMapped.map(_._2.get)
    Some(
      Environment(
        environmentFinal(0).split(",").toList,
        environmentFinal(1),
        environmentFinal(2),
        environmentFinal(3),
        environmentFinal(4).split(",").toList,
        environmentFinal(5).split(",").toList,
        environmentFinal(6).toLong
      ))
  }

}
