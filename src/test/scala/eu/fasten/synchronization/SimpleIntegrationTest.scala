package eu.fasten.synchronization

import java.util.concurrent.ExecutionException

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.flink.runtime.JobException
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class SimpleIntegrationTest
    extends AnyFunSuite
    with EmbeddedKafka
    with BeforeAndAfter {

  before {
    val customBrokerConfig = Map("transaction.max.timeout.ms" -> "3600000")
    val customProducerConfig = Map("" -> "")
    val customConsumerConfig = Map("" -> "")

    implicit val customKafkaConfig =
      EmbeddedKafkaConfig(customBrokerProperties = customBrokerConfig,
                          customProducerProperties = customProducerConfig,
                          customConsumerProperties = customConsumerConfig)
    setAllEnv()
    EmbeddedKafka.start()
  }
  test("Check proper parsing of messages") {
    val repoClonerMsg: String =
      Source.fromResource("repocloner_msg.json").getLines.mkString

    val metadataMsg: String =
      Source.fromResource("metadatadb_msg.json").getLines.mkString

    setEnv("MAX_RECORDS", "2")
    publishStringMessageToKafka("repocloner.out", repoClonerMsg)
    publishStringMessageToKafka("metadata.out", metadataMsg)
    publishStringMessageToKafka("repocloner.out", "{}")

    assertThrows[ExecutionException] {
      Main.main(Array[String]())
    }

  }

  def setAllEnv(): Unit = {
    setEnv("KAFKA_BROKER", "localhost:6001")
    setEnv("INPUT_TOPIC_ONE", "repocloner.out")
    setEnv("INPUT_TOPIC_TWO", "metadata.out")
    setEnv("OUTPUT_TOPIC", "output.out")
    setEnv("TOPIC_ONE_KEYS",
           "input.input.groupId,input.input.artifactId,input.input.version")
    setEnv(
      "TOPIC_TWO_KEYS",
      "input.input.input.groupId,input.input.input.artifactId,input.input.input.version")
    setEnv("WINDOW_TIME", "99")
  }

  def setEnv(key: String, value: String) = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    val map = field
      .get(System.getenv())
      .asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]
    map.put(key, value)
  }

  after {
    EmbeddedKafka.stop()
  }

}
