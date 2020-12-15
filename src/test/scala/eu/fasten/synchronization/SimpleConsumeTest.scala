package eu.fasten.synchronization

import java.util.concurrent.ExecutionException

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.flink.runtime.JobException
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class SimpleConsumeTest
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
    EmbeddedKafka.start()
  }
  test("Check proper parsing of messages") {
    val repoClonerMsg: String =
      Source.fromResource("repocloner_msg.json").getLines.mkString

    val metadataMsg: String =
      Source.fromResource("metadatadb_msg.json").getLines.mkString

    publishStringMessageToKafka("repocloner.out", repoClonerMsg)
    publishStringMessageToKafka("metadata.out", metadataMsg)
    publishStringMessageToKafka("repocloner.out", "{}")

    assertThrows[ExecutionException] {
      Main.main(getStartArg())
    }

  }

  after {
    EmbeddedKafka.stop()
  }

  def getStartArg(): Array[String] = {
    "-b localhost:6001 --topic_one repocloner.out --topic_two metadata.out -o output.out --topic_one_keys input.input.groupId,input.input.artifactId,input.input.version --topic_two_keys input.input.input.groupId,input.input.input.artifactId,input.input.input.version -w 3600 --max_records 2 --delay_topic delay --enable_delay true"
      .split(" ")
  }

}
