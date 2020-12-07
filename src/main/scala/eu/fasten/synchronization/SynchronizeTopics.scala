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
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

class SynchronizeTopics(c: Config)
    extends KeyedProcessFunction[String, ObjectNode, ObjectNode] {

  val logger = Logger(getClass.getSimpleName)

  // This is just a sanity check, state is removed after 2 times the window time.
  val stateTtlConfig = StateTtlConfig
    .newBuilder(Time.seconds(c.windowTime * 2))
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
    .cleanupInRocksdbCompactFilter(10000)
    .build

  // State for topicOne.
  val topicOneStateDescriptor = new ValueStateDescriptor[ObjectNode](
    c.topicOne + "_state",
    classOf[ObjectNode])
  topicOneStateDescriptor.enableTimeToLive(stateTtlConfig)

  lazy val topicOneState: ValueState[ObjectNode] =
    getRuntimeContext.getState(topicOneStateDescriptor)

  // State for topicTwo
  val topicTwoStateDescriptor = new ValueStateDescriptor[ObjectNode](
    c.topicTwo + "_state",
    classOf[ObjectNode])
  topicTwoStateDescriptor.enableTimeToLive(stateTtlConfig)

  lazy val topicTwoState: ValueState[ObjectNode] =
    getRuntimeContext.getState(topicTwoStateDescriptor)

  // A Jackson mapper, to create JSON objects.
  lazy val mapper: ObjectMapper = new ObjectMapper()

  //For the error output.
  val errOutputTag = OutputTag[ObjectNode]("err-output")

  override def processElement(
      value: ObjectNode,
      ctx: KeyedProcessFunction[String, ObjectNode, ObjectNode]#Context,
      out: Collector[ObjectNode]): Unit = {

    val topic = value
      .get("metadata")
      .get("topic")
      .asText()

    logger.info(
      f"[INCOMING] [${ctx.getCurrentKey}] [$topic] [${ctx.timestamp()}i] [-1i] [NONE]")

    if (topic == c.topicOne) {
      handleRecord(topic,
                   topicOneState,
                   c.topicTwo,
                   topicTwoState,
                   value,
                   ctx,
                   out)

    } else if (topic == c.topicTwo) {
      handleRecord(topic,
                   topicTwoState,
                   c.topicOne,
                   topicOneState,
                   value,
                   ctx,
                   out)
    }
  }

  def handleRecord(
      thisTopic: String,
      thisTopicState: ValueState[ObjectNode],
      otherTopic: String,
      otherTopicState: ValueState[ObjectNode],
      value: ObjectNode,
      ctx: KeyedProcessFunction[String, ObjectNode, ObjectNode]#Context,
      out: Collector[ObjectNode]): Unit = {

    // Current time and the time of the event.
    val currentTime = System.currentTimeMillis()
    val timestamp = ctx.timestamp()

    /**
      *  Check state of the other topic.
      *  1) if not emtpy, emit both values.
      *  2) if empty, add this message to the state of this topic.
      */
    val otherTopicCurrentState = otherTopicState.value()

    // We already have the data from the other topic! Join time :)
    if (otherTopicCurrentState != null) {
      val stateTimestamp = otherTopicCurrentState
        .get("state_timestamp")
        .asLong()

      // Compute how long it took, to join both records.
      val duration = currentTime - stateTimestamp

      logger.info(
        f"[JOIN] [${ctx.getCurrentKey}] [BOTH] [${value.get("metadata").get("timestamp").asText()}i] [${duration}i] [NONE]")

      // Get timestamp of the record from topic two.
      val otherTopicRecordTimestamp =
        otherTopicCurrentState.get("metadata").get("timestamp").asLong()

      // Remove metadata.
      otherTopicCurrentState.remove("state_timestamp")
      otherTopicCurrentState.remove("metadata")
      value.remove("metadata")

      // Build an output record.
      val outputRecord = mapper.createObjectNode()
      outputRecord.put("key", ctx.getCurrentKey)
      outputRecord.set(otherTopic, otherTopicCurrentState.get("value"))
      outputRecord.set(thisTopic, value.get("value"))

      // Collect the record.
      out.collect(outputRecord)

      // Remove the timer associated with this state.
      ctx
        .timerService()
        .deleteEventTimeTimer(otherTopicRecordTimestamp + (c.windowTime * 1000))

      // Empty the state of both, just to be safe.
      otherTopicState.clear()
      thisTopicState.clear()

      return
    } else { // The state in topic two is still empty, let's add to state one.
      if (thisTopicState
            .value() != null) { // There is a duplicate, because the state is not null. We just override it.
        val duplicateDuration = currentTime - thisTopicState
          .value()
          .get("state_timestamp")
          .asLong()
        logger.warn(
          f"[DUPLICATE] [${ctx.getCurrentKey}] [$thisTopic] [${value.get("metadata").get("timestamp").asText()}i] [${duplicateDuration}i] [NONE]")

        ctx
          .timerService()
          .deleteEventTimeTimer(
            value
              .get("metadata")
              .get("timestamp")
              .asLong() + (c.windowTime * 1000))
      }

      // Update state, add field with current timestamp (that's nice to know for the monitoring).
      value.put("state_timestamp", currentTime.toString)
      thisTopicState.update(value)

      // Set a timer, to ensure that this message is joined within {windowTime} seconds.
      ctx
        .timerService()
        .registerEventTimeTimer(ctx.timestamp() + (c.windowTime * 1000))
    }
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[String, ObjectNode, ObjectNode]#OnTimerContext,
      out: Collector[ObjectNode]): Unit = {
    // A timer is created when state is added. If this timer is called, we need to expire the state and output an error.

    val currentTime = System.currentTimeMillis()

    val topicOneCurrentState = topicOneState.value()
    val topicTwoCurrentState = topicTwoState.value()

    if (topicOneCurrentState != null && topicTwoCurrentState != null) { // Both states are filled, this should not be possible.
      val duration = Math.max(
        topicOneCurrentState.get("state_timestamp").asLong(),
        topicTwoCurrentState.get("state_timestamp").asLong()) - Math.min(
        topicOneCurrentState.get("state_timestamp").asLong(),
        topicTwoCurrentState.get("state_timestamp").asLong())
      logger.warn(
        f"[EXPIRE] [${ctx.getCurrentKey}] [BOTH] [${topicOneCurrentState.get("metadata").get("timestamp").asText()}i] [${duration}i] [NONE]")

      // Remove metadata.
      topicOneCurrentState.remove("state_timestamp")
      topicOneCurrentState.remove("metadata")
      topicTwoCurrentState.remove("state_timestamp")
      topicTwoCurrentState.remove("metadata")

      // Build an output record.
      val outputRecord = mapper.createObjectNode()
      outputRecord.put("key", ctx.getCurrentKey)
      outputRecord.set(c.topicOne, topicOneCurrentState.get("value"))
      outputRecord.set(c.topicTwo, topicTwoCurrentState.get("value"))

      // Collect the record.
      out.collect(outputRecord)

    } else if (topicOneCurrentState != null) {

      // Build an output record.
      val outputRecord = mapper.createObjectNode()
      outputRecord.put("key", ctx.getCurrentKey)
      outputRecord.put("error", f"Missing information from ${c.topicTwo}")
      outputRecord.set(c.topicOne, topicOneCurrentState.get("value"))

      val stateTimestamp = topicOneCurrentState
        .get("state_timestamp")
        .asLong()

      // Compute how long it took, to join both records.
      val duration = currentTime - stateTimestamp

      // side output
      ctx.output(errOutputTag, outputRecord)

      logger.warn(
        f"[EXPIRE] [${ctx.getCurrentKey}] [${c.topicOne}] [${topicOneCurrentState.get("metadata").get("timestamp").asText()}i] [${duration}i] [NONE]")

    } else if (topicTwoCurrentState != null) {

      // Build an output record.
      val outputRecord = mapper.createObjectNode()
      outputRecord.put("key", ctx.getCurrentKey)
      outputRecord.put("error", f"Missing information from ${c.topicOne}")
      outputRecord.set(c.topicTwo, topicTwoCurrentState.get("value"))

      val stateTimestamp = topicTwoCurrentState
        .get("state_timestamp")
        .asLong()

      // Compute how long it took, to join both records.
      val duration = currentTime - stateTimestamp

      // side output
      ctx.output(errOutputTag, outputRecord)

      logger.warn(
        f"[EXPIRE] [${ctx.getCurrentKey}] [${c.topicTwo}] [${topicTwoCurrentState.get("metadata").get("timestamp").asText()}i] [${duration}i] [NONE]")
    } else {
      logger.warn(f"[EXPIRE] [${ctx.getCurrentKey}] [NONE] [0i] [-1i] [NONE]")
    }

    topicOneState.clear()
    topicTwoState.clear()
  }
}
