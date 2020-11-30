package eu.fasten.synchronization

import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.state.{
  MapState,
  MapStateDescriptor,
  StateTtlConfig,
  ValueState,
  ValueStateDescriptor
}
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.JobException
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector

class SynchronizeTopics(environment: Environment)
    extends KeyedProcessFunction[String, ObjectNode, ObjectNode, ObjectNode] {

  val logger = Logger(getClass.getSimpleName)

  // This is just a sanity check, state is removed after 2 times the window time.
  val stateTtlConfig = StateTtlConfig
    .newBuilder(Time.seconds(environment.windowTime * 2))
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
    .cleanupInRocksdbCompactFilter(10000)
    .build

  // State for topicOne.
  val topicOneStateDescriptor = new ValueStateDescriptor[ObjectNode](
    environment.topicTwo + "_state",
    classOf[ObjectNode])
  topicOneStateDescriptor.enableTimeToLive(stateTtlConfig)

  lazy val topicOneState: ValueState[ObjectNode] =
    getRuntimeContext.getState(topicOneStateDescriptor)

  // State for topicTwo
  val topicTwoStateDescriptor = new ValueStateDescriptor[ObjectNode](
    environment.topicTwo + "_state",
    classOf[ObjectNode])
  topicTwoStateDescriptor.enableTimeToLive(stateTtlConfig)

  lazy val topicTwoState: ValueState[ObjectNode] =
    getRuntimeContext.getState(topicTwoStateDescriptor)

  // A Jackson mapper, to create JSON objects.
  lazy val mapper: ObjectMapper = new ObjectMapper()

  override def processElement(
      value: ObjectNode,
      ctx: KeyedProcessFunction[String, ObjectNode, ObjectNode]#Context,
      out: Collector[ObjectNode]): Unit = {

    val topic = value
      .get("metadata")
      .get("topic")
      .asText()

    logger.info(
      f"[INCOMING] [${ctx.getCurrentKey}] [$topic] [${value.get("metadata").get("timestamp").asText()}] [-1i] [NONE]")

    if (topic == environment.topicOne) {

      /**
        * SCENARIO 1: Check state of topic two.
        *  1) if not emtpy, emit both values.
        *  2) if empty, add this message to the state of topic one.
        */
      val topicTwoCurrentState = topicTwoState.value()

      // We already have the data from the other topic! Join time :)
      if (topicTwoCurrentState != null) {
        val currentTime = System.currentTimeMillis()
        val stateTimestamp = topicTwoCurrentState
          .get("state_timestamp")
          .asLong()

        // Compute how long it took, to join both records.
        val duration = currentTime - stateTimestamp

        logger.info(
          f"[JOIN] [${ctx.getCurrentKey}] [BOTH] [${value.get("metadata").get("timestamp").asText()}] [${duration}i] [NONE]")

        // Remove metadata.
        topicTwoCurrentState.remove("state_timestamp")
        topicTwoCurrentState.remove("metadata")
        value.remove("metadata")

        // Build an output record.
        val outputRecord = mapper.createObjectNode()
        outputRecord.put("key", ctx.getCurrentKey)
        outputRecord.set(environment.topicTwo, value)
        outputRecord.set(environment.topicOne, topicTwoCurrentState)

        // Collect the record.
        out.collect(outputRecord)

        // Remove the timer associated with this state.
        ctx
          .timerService()
          .deleteEventTimeTimer(
            stateTimestamp + (environment.windowTime * 1000))

        // Empty the state.
        topicTwoState.update(null)

        return
      } else { // The state in topic two is still empty, let's add to state one.
        if (topicOneState.value() != null) { // There is a duplicate, because the state is not null. We just override.

        }
      }

    } else if (topic == environment.topicTwo) {} else {

      val exception = new JobException(
        s"Expected a message from ${environment.topicOne} or ${environment.topicTwo}, but received from $topic.")
      logger.info(
        f"[INCOMING] [${ctx.getCurrentKey}] [$topic] [${value.get("metadata").get("timestamp").asText()} [-1i] [JobException]",
        exception)

      throw exception
    }

  }
}
