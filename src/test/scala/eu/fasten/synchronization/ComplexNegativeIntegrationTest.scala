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

class ComplexNegativeIntegrationTest
    extends AnyFunSuite
    with EmbeddedKafka
    with BeforeAndAfter {

  before {
    setAllEnv()
    EmbeddedKafka.start()
  }

  test("Message from first topic is not coming at all, counter will timeout.") {
    val repoClonerMsg: String =
      Source.fromResource("repocloner_msg.json").getLines.mkString
    val repoCloner2Msg: String =
      Source.fromResource("repocloner_1_msg.json").getLines.mkString

    setEnv("MAX_RECORDS", "101")
    val repoClonerRecord =
      new ProducerRecord(
        "repocloner.out",
        null,
        System.currentTimeMillis() - (4 * 3600000), //3 hours ago
        "{}",
        repoClonerMsg)

    val repoClonerRecordLater =
      new ProducerRecord(
        "repocloner.out",
        null,
        System.currentTimeMillis() - (1 * 3600000), //1 hours ago
        "{}",
        repoCloner2Msg)

    implicit val serializer = new StringSerializer
    publishToKafka(repoClonerRecord)

    val system = ActorSystem.create("simple_delay")
    system.scheduler.scheduleOnce(5 seconds) {
      for (i <- 1 to 100) {
        publishToKafka(repoClonerRecordLater)
      }
    }

    system.scheduler.scheduleOnce(7 seconds) {
      //triggers the app to stop.
      publishStringMessageToKafka("repocloner.out", "{}")
    }

    assertThrows[ExecutionException] {
      Main.main(Array[String]())
    }

    println("HI")
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
    setEnv("WINDOW_TIME", "3600")
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
