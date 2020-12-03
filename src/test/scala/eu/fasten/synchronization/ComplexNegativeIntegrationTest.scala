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
    EmbeddedKafka.start()
  }

  test("Message from first topic is not coming at all, counter will timeout.") {
    val repoClonerMsg: String =
      Source.fromResource("repocloner_msg.json").getLines.mkString
    val repoCloner2Msg: String =
      Source.fromResource("repocloner_1_msg.json").getLines.mkString

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
      publishToKafka(repoClonerRecordLater)
    }

    system.scheduler.scheduleOnce(7 seconds) {
      //triggers the app to stop.
      publishStringMessageToKafka("repocloner.out", "{}")
    }

    assertThrows[ExecutionException] {
      Main.main(getStartArg())
    }

    println("Test finished")
  }

  after {
    EmbeddedKafka.stop()
  }

  def getStartArg(): Array[String] = {
    "-b localhost:6001 --topic_one repocloner.out --topic_two metadata.out -o output.out --topic_one_keys input.input.groupId,input.input.artifactId,input.input.version --topic_two_keys input.input.input.groupId,input.input.input.artifactId,input.input.input.version -w 3600 --max_records 2"
      .split(" ")
  }

}
