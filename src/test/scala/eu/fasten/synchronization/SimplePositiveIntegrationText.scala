package eu.fasten.synchronization

import java.util.concurrent.ExecutionException

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.JsonNodeDeserializer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{
  StringDeserializer,
  StringSerializer
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class SimplePositiveIntegrationText
    extends AnyFunSuite
    with BeforeAndAfterAll
    with EmbeddedKafka {

  override def beforeAll() {
    EmbeddedKafka.start()

    val repoClonerMsg: String =
      Source.fromResource("repocloner_msg.json").getLines.mkString

    val metadataMsg: String =
      Source.fromResource("metadatadb_msg.json").getLines.mkString

    val repoClonerRecord = new ProducerRecord("repocloner.out",
                                              null,
                                              System.currentTimeMillis(),
                                              "{}",
                                              repoClonerMsg)
    val metaDataRecord = new ProducerRecord("metadata.out",
                                            null,
                                            System.currentTimeMillis(),
                                            "{}",
                                            metadataMsg)

    implicit val serializer = new StringSerializer
    publishToKafka(repoClonerRecord)
    publishToKafka(metaDataRecord)

    publishStringMessageToKafka("repocloner.out", "{}")
  }

  test("Two records in Kafka, very simple join.") {
    assertThrows[ExecutionException] {
      Main.main(getStartArg())
    }

    implicit val deserializer = new StringDeserializer
    val message = consumeFirstMessageFrom("fasten.output.out")

    val messageParsed = new ObjectMapper().readValue(message, classOf[JsonNode])

    println(s"Consumed message ${message}")

    assert(messageParsed.get("repocloner.out") != null)
    assert(messageParsed.get("metadata.out") != null)
    assert(messageParsed.get("non_existent.out") == null)
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
  }

  def getStartArg(): Array[String] = {
    "-b localhost:6001 --topic_one repocloner.out --topic_two metadata.out -o output --topic_one_keys input.input.groupId,input.input.artifactId,input.input.version --topic_two_keys input.input.input.groupId,input.input.input.artifactId,input.input.input.version -w 3600 --max_records 2 --delay_topic delay"
      .split(" ")
  }

}
