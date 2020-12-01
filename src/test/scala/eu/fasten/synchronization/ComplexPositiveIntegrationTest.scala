package eu.fasten.synchronization

import java.util.concurrent.ExecutionException

import akka.actor.ActorSystem
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{
  StringDeserializer,
  StringSerializer
}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.io.Source

class ComplexPositiveIntegrationTest
    extends AnyFunSuite
    with EmbeddedKafka
    with BeforeAndAfter {

  before {
    setAllEnv()
    EmbeddedKafka.start()
  }

  test("Message from first topic is a few seconds later.") {
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

    val system = ActorSystem.create("simple_delay")
    system.scheduler.scheduleOnce(5 seconds) {
      publishToKafka(metaDataRecord)
      //triggers the app to stop.
      publishStringMessageToKafka("repocloner.out", "{}")
    }

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

  test("Message from second topic is a few seconds later.") {
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
    publishToKafka(metaDataRecord)

    val system = ActorSystem.create("simple_delay")
    system.scheduler.scheduleOnce(5 seconds) {
      publishToKafka(repoClonerRecord)
    }

    system.scheduler.scheduleOnce(7 seconds) {
      publishStringMessageToKafka("repocloner.out", "{}")
    }

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

  after {
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
