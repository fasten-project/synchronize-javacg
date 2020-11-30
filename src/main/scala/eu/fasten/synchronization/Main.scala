package eu.fasten.synchronization

import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/** Case class to store environment variables.
  *
  * @param brokers the Kafka brokers to connect to.
  * @param topicOne the first topic to read from.
  * @param topicTwo the second topic to read from.
  * @param keys the keys to join on.
  * @param windowTime the amount of time elements are stored in state.
  */
case class Environment(brokers: List[String],
                       topicOne: String,
                       topicTwo: String,
                       outputTopic: String,
                       keys: List[String],
                       windowTime: Long)

object Main {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val logger = Logger("Main")

  def main(args: Array[String]): Unit = {
    // We need to ensure, we have the correct environment variables.
    val loadedEnv = verifyEnvironment()
    if (loadedEnv.isEmpty) {
      System.exit(1)
    } else {
      logger.info(s"Loaded environment: ${loadedEnv}")
    }

    env
      .fromCollection(List("This is just a test"))
      .flatMap(_.split(" "))
      .print()

    env.execute()
  }

  def verifyEnvironment(): Option[Environment] = {

    val inputEnv =
      List("KAFKA_BROKER",
           "INPUT_TOPIC_ONE",
           "INPUT_TOPIC_TWO",
           "OUTPUT_TOPIC",
           "JOIN_KEYS",
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
        environmentFinal(5).toLong
      ))
  }

}
