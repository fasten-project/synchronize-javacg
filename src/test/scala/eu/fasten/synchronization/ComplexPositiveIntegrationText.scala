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

class ComplexPositiveIntegrationText
    extends AnyFunSuite
    with BeforeAndAfterAll
    with EmbeddedKafka {

  override def beforeAll() {
    setAllEnv()
    EmbeddedKafka.start()

    val repoClonerMsg: String =
      Source.fromResource("repocloner_msg.json").getLines.mkString

    val metadataMsg: String =
      Source.fromResource("metadatadb_msg.json").getLines.mkString

    setEnv("MAX_RECORDS", "2")
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
      Main.main(Array[String]())
    }

    implicit val deserializer = new StringDeserializer
    val message = consumeFirstMessageFrom("output.out")

    val messageParsed = new ObjectMapper().readValue(message, classOf[JsonNode])

    println(s"Consumed message ${message}")

    assert(messageParsed.get("repocloner.out") != null)
    assert(messageParsed.get("metadata.out") != null)
    assert(messageParsed.get("non_existent.out") == null)
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
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
    setEnv("WINDOW_TIME", "100")
  }

  def setEnv(key: String, value: String) = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    val map = field
      .get(System.getenv())
      .asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]
    map.put(key, value)
  }
}
