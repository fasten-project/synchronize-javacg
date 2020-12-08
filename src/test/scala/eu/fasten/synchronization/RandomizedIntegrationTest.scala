package eu.fasten.synchronization

import java.util.{Collections, UUID}

import akka.actor.ActorSystem

import scala.util.Random
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{
  Deserializer,
  StringDeserializer,
  StringSerializer
}
import org.scalatest.concurrent.Eventually
import org.scalatest.{Assertion, BeforeAndAfter}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.ExecutionException
import scala.collection.JavaConversions._

class RandomizedIntegrationTest
    extends AnyFunSuite
    with BeforeAndAfter
    with EmbeddedKafka
    with Eventually
    with Matchers {

  val objectMapper: ObjectMapper = new ObjectMapper()
  val rnd: Random = new Random()

  before {
    EmbeddedKafka.start()

  }

  test("Randomized full integration test.") {
    // First we create 25 un-joinable records in repo.cloner.
    val unjoinableRepo: List[ProducerRecord[String, String]] = {
      for (i <- 1 to 25)
        yield
          buildRecord(
            "repocloner.out",
            UUID.randomUUID().toString,
            "wouter",
            "1.1.1",
            System.currentTimeMillis() - (4 * 3600000) - betweenRand(
              0,
              3600000)) // Set time in between 3 and 4 hours ago. Messages won't be in order, but we allow for 1 hour out-of-orderness in Flink.
    }.toList

    val joinableUUIDs: List[String] =
      List.range(0, 25).map(x => UUID.randomUUID().toString)

    val joinableRepo: List[ProducerRecord[String, String]] = joinableUUIDs.map(
      buildRecord(
        "repocloner.out",
        _,
        "wouter",
        "1.0",
        System.currentTimeMillis() - (3 * 3600000) - betweenRand(0, 3600000)))

    val joinableMetadataOut: List[ProducerRecord[String, String]] =
      joinableUUIDs.map(
        buildRecord(
          "metadata.out",
          _,
          "wouter",
          "1.0",
          System.currentTimeMillis() - (3 * 3600000) - betweenRand(0, 3600000)))

    val unjoinableMetadata: List[ProducerRecord[String, String]] = {
      for (i <- 1 to 25)
        yield
          buildRecord(
            "metadata.out",
            UUID.randomUUID().toString,
            "wouter",
            "1.1.1",
            System.currentTimeMillis() - (2 * 3600000) - betweenRand(
              0,
              3600000)) // Set time in between 3 and 4 hours ago. Messages won't be in order, but we allow for 1 hour out-of-orderness in Flink.
    }.toList

    val joinableUUIDs2: List[String] =
      List.range(0, 25).map(x => UUID.randomUUID().toString)

    val joinableRepo2: List[ProducerRecord[String, String]] =
      joinableUUIDs2.map(
        buildRecord(
          "repocloner.out",
          _,
          "wouter",
          "1.0",
          System.currentTimeMillis() - (1 * 3600000) - betweenRand(0, 3600000)))

    val joinableMetadataOut2: List[ProducerRecord[String, String]] =
      joinableUUIDs2.map(
        buildRecord(
          "metadata.out",
          _,
          "wouter",
          "1.0",
          System.currentTimeMillis() - (1 * 3600000) - betweenRand(0, 3600000)))

    // We need this record, in order to get a new watermark and all data already in the system properly processed.
    val recordToFixWatermark = buildRecord("repocloner.out",
                                           UUID.randomUUID().toString,
                                           "wouter",
                                           "1.1.1",
                                           System.currentTimeMillis())

    val recordToFixWatermarkMeta = buildRecord("metadata.out",
                                               UUID.randomUUID().toString,
                                               "wouter",
                                               "1.1.1",
                                               System.currentTimeMillis())

    // Start sending
    implicit val serializer = new StringSerializer
    unjoinableRepo.foreach(publishToKafka(_))
    joinableMetadataOut.foreach(publishToKafka(_))
    joinableRepo.foreach(publishToKafka(_))
    unjoinableMetadata.foreach(publishToKafka(_))
    joinableMetadataOut2.foreach(publishToKafka(_))
    joinableRepo2.foreach(publishToKafka(_))

    // Stop after a few seconds
    val system = ActorSystem.create("fix_watermark_and_stop")
    system.scheduler.scheduleOnce(6 seconds) {
      publishToKafka(recordToFixWatermark)
      publishToKafka(recordToFixWatermarkMeta)
    }
    system.scheduler.scheduleOnce(9 seconds) {
      publishStringMessageToKafka("repocloner.out", "{}")
    }

    assertThrows[ExecutionException] {
      Main.main(getStartArg())
    }

    implicit val deserializer: Deserializer[String] = new StringDeserializer()
    consumeNumberMessagesFrom("fasten.delay.out", 50)
    val outputUUID =
      consumeNumberMessagesFrom("fasten.output.out", 50).toList.map { x =>
        objectMapper
          .readValue(x, classOf[JsonNode])
          .get("key")
          .asText
          .split(":")(0)
      }.toSet

    assert(outputUUID == (joinableUUIDs.toSet.union(joinableUUIDs2.toSet)))
  }

  def buildRecord(topic: String,
                  groupId: String,
                  artifactId: String,
                  version: String,
                  timestamp: Long) = {

    val obj = objectMapper.createObjectNode()

    if (topic == "repocloner.out") {
      obj
        .putObject("input")
        .putObject("input")
        .put("groupId", groupId)
        .put("artifactId", artifactId)
        .put("version", version)
    } else {
      obj
        .putObject("input")
        .putObject("input")
        .putObject("input")
        .put("groupId", groupId)
        .put("artifactId", artifactId)
        .put("version", version)
    }

    new ProducerRecord(topic, null, timestamp, "{}", obj.toString)
  }

  def getStartArg(): Array[String] = {
    "-b localhost:6001 --topic_one repocloner.out --topic_two metadata.out -o output --topic_one_keys input.input.groupId,input.input.artifactId,input.input.version --topic_two_keys input.input.input.groupId,input.input.input.artifactId,input.input.input.version -w 3600 --delay_topic delay --max_records 152"
      .split(" ")
  }

  def betweenRand(start: Int, end: Int) = {
    start + rnd.nextInt((end - start) + 1)
  }

  after {
    EmbeddedKafka.stop()
  }
}
