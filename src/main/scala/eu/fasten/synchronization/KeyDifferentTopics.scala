package eu.fasten.synchronization

import com.typesafe.scalalogging.Logger
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.runtime.JobException
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode

class KeyDifferentTopics(c: Config) extends KeySelector[ObjectNode, String] {

  val logger = Logger(getClass.getSimpleName)
  val keySeparator = ":"

  override def getKey(value: ObjectNode): String = {
    val topic = value
      .get("metadata")
      .get("topic")
      .asText()

    if (topic == c.topicOne) {
      getKeyFromTopic(c.topicOneKeys.toList, value)
    } else if (topic == c.topicTwo) {
      getKeyFromTopic(c.topicTwoKeys.toList, value)
    } else {
      val exception = new JobException(
        s"Expected a message from ${c.topicOne} or ${c.topicTwo}, but received from $topic.")
      logger.info(
        f"[INCOMING] [NONE] [$topic] [${value.get("metadata").get("timestamp").asText()}i] [-1i] [JobException]",
        exception)

      throw exception
    }
  }

  def getKeyFromTopic(topicKeys: List[String], value: ObjectNode): String = {
    val keyValues = topicKeys
      .map(key => "/value/" + key.split("\\.").mkString("/"))
      .map(value.at(_).asText())

    keyValues.mkString(keySeparator)
  }
}
