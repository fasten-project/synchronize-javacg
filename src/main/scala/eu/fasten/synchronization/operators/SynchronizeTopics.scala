package eu.fasten.synchronization.operators

import com.typesafe.scalalogging.Logger
import eu.fasten.synchronization.Config
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector

case class TopicState(topic: String, state: ValueState[ObjectNode])

class SynchronizeTopics(c: Config)
    extends KeyedProcessFunction[String, ObjectNode, ObjectNode] {

  val logger = Logger(getClass.getSimpleName)

  // Join already done state.
  val joinDoneStateDescriptor =
    new ValueStateDescriptor[Boolean]("join_done_state", classOf[Boolean])
  lazy val joinDoneState: ValueState[Boolean] =
    getRuntimeContext.getState(joinDoneStateDescriptor)

  // State for topicOne.
  val topicOneStateDescriptor = new ValueStateDescriptor[ObjectNode](
    c.topicOne + "_state",
    classOf[ObjectNode])

  lazy val topicOneState: ValueState[ObjectNode] =
    getRuntimeContext.getState(topicOneStateDescriptor)

  // State for topicTwo
  val topicTwoStateDescriptor = new ValueStateDescriptor[ObjectNode](
    c.topicTwo + "_state",
    classOf[ObjectNode])

  lazy val topicTwoState: ValueState[ObjectNode] =
    getRuntimeContext.getState(topicTwoStateDescriptor)

  // A Jackson mapper, to create JSON objects.
  lazy val mapper: ObjectMapper = new ObjectMapper()

  //For the delayed output.
  val delayOutputTag = OutputTag[ObjectNode]("delay-output")

  override def processElement(
      value: ObjectNode,
      ctx: KeyedProcessFunction[String, ObjectNode, ObjectNode]#Context,
      out: Collector[ObjectNode]): Unit = {

    val topic = value
      .get("metadata")
      .get("topic")
      .asText()

    logger.info(
      f"[INCOMING] [${ctx.getCurrentKey}] [$topic] [${ctx.timestamp()}i] [-1] [NONE]")

    // The join is already done, we ignore and drop the record.
    if (joinAlreadyDone()) {
      logger.info(
        f"[DUPLICATE] [${ctx.getCurrentKey}] [$topic] [${ctx.timestamp()}i] [-1] [JoinAlreadyDoneException]")
      return
    }

    if (topic == c.topicOne) {
      handleRecord(TopicState(topic, topicOneState),
                   TopicState(c.topicTwo, topicTwoState),
                   value,
                   ctx,
                   out)

    } else if (topic == c.topicTwo) {
      handleRecord(TopicState(topic, topicTwoState),
                   TopicState(c.topicOne, topicOneState),
                   value,
                   ctx,
                   out)
    }
  }

  def handleRecord(
      thisTopicState: TopicState,
      otherTopicState: TopicState,
      value: ObjectNode,
      ctx: KeyedProcessFunction[String, ObjectNode, ObjectNode]#Context,
      out: Collector[ObjectNode]): Unit = {

    // Current time and the time of the event.
    val currentTime = System.currentTimeMillis()

    /**
      *  Check state of the other topic.
      *  1) if not emtpy, emit both values.
      *  2) if empty, add this message to the state of this topic.
      */
    val otherTopicCurrentState = otherTopicState.state.value()

    // We already have the data from the other topic! Join time :)
    if (otherTopicCurrentState != null) {
      val stateTimestamp = otherTopicCurrentState
        .get("state_timestamp")
        .asLong()

      // Compute how long it took, to join both records.
      val duration = currentTime - stateTimestamp

      logger.info(
        f"[JOIN] [${ctx.getCurrentKey}] [BOTH] [${value.get("metadata").get("timestamp").asText()}i] [${duration}] [NONE]")

      // Get timestamp of the record from topic two.
      val otherTopicRecordTimestamp =
        otherTopicCurrentState.get("metadata").get("timestamp").asLong()

      // Collect the record.
      out.collect(
        buildSuccessRecord(otherTopicCurrentState,
                           value,
                           otherTopicState.topic,
                           thisTopicState.topic,
                           ctx))

      // Remove the timer associated with this state.
      if (c.enableDelay) {
        ctx
          .timerService()
          .deleteEventTimeTimer(
            otherTopicRecordTimestamp + (c.windowTime * 1000))
      }

      // Empty the state of both, just to be safe.
      otherTopicState.state.clear()
      thisTopicState.state.clear()

      // Update the state to capture that join is already done.
      doJoin()

      return
    } else { // The state in topic two is still empty, let's add to state one.
      if (thisTopicState.state
            .value() != null) { // There is a duplicate, because the state is not null. We just override it.
        val duplicateDuration = currentTime - thisTopicState.state
          .value()
          .get("state_timestamp")
          .asLong()
        logger.warn(
          f"[DUPLICATE-OVERRIDE] [${ctx.getCurrentKey}] [${thisTopicState.topic}] [${value.get("metadata").get("timestamp").asText()}i] [${duplicateDuration}] [NONE]")

        if (c.enableDelay) {
          ctx
            .timerService()
            .deleteEventTimeTimer(
              value
                .get("metadata")
                .get("timestamp")
                .asLong() + (c.windowTime * 1000))
        }
      }

      // Update state, add field with current timestamp (that's nice to know for the monitoring).
      value.put("state_timestamp", currentTime.toString)
      thisTopicState.state.update(value)

      if (c.enableDelay) {
        // Set a timer, to ensure that this message is joined within {windowTime} seconds.
        ctx
          .timerService()
          .registerEventTimeTimer(ctx.timestamp() + (c.windowTime * 1000))
      }
    }
  }

  def buildSuccessRecord(
      otherTopicCurrentState: ObjectNode,
      value: ObjectNode,
      otherTopic: String,
      thisTopic: String,
      ctx: KeyedProcessFunction[String, ObjectNode, ObjectNode]#Context)
    : ObjectNode = {
    // Remove metadata.
    otherTopicCurrentState.remove("state_timestamp")
    otherTopicCurrentState.remove("metadata")
    value.remove("metadata")

    // Build an output record.
    val outputRecord = mapper.createObjectNode()
    outputRecord.put("key", ctx.getCurrentKey)
    outputRecord.set(otherTopic, otherTopicCurrentState.get("value"))
    outputRecord.set(thisTopic, value.get("value"))

    outputRecord
  }

  def doJoin() = {
    joinDoneState.update(true)
  }

  def joinAlreadyDone(): Boolean = {
    if (joinDoneState.value() == null) {
      return false
    }

    joinDoneState.value()
  }

  def buildSuccessRecordOnTimer(
      topicOneCurrentState: ObjectNode,
      topicTwoCurrentState: ObjectNode,
      ctx: KeyedProcessFunction[String, ObjectNode, ObjectNode]#OnTimerContext)
    : ObjectNode = {
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

    outputRecord
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[String, ObjectNode, ObjectNode]#OnTimerContext,
      out: Collector[ObjectNode]): Unit = {
    // A timer is created when state is added. If this timer is called, we need to send the state as a delay and output an error.

    val currentTime = System.currentTimeMillis()

    val topicOneCurrentState = topicOneState.value()
    val topicTwoCurrentState = topicTwoState.value()

    if (topicOneCurrentState != null && topicTwoCurrentState != null) { // Both states are filled, this should not be possible.
      val duration = Math.max(
        topicOneCurrentState.get("state_timestamp").asLong(),
        topicTwoCurrentState.get("state_timestamp").asLong()) - Math.min(
        topicOneCurrentState.get("state_timestamp").asLong(),
        topicTwoCurrentState.get("state_timestamp").asLong())
      logger.info(
        f"[JOIN] [${ctx.getCurrentKey}] [BOTH] [${topicOneCurrentState.get("metadata").get("timestamp").asText()}i] [${duration}] [DELAY]")

      // Collect the record.
      out.collect(
        buildSuccessRecordOnTimer(topicOneCurrentState,
                                  topicTwoCurrentState,
                                  ctx))

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
      ctx.output(delayOutputTag, outputRecord)

      logger.info(
        f"[DELAY] [${ctx.getCurrentKey}] [${c.topicOne}] [${topicOneCurrentState.get("metadata").get("timestamp").asText()}i] [${duration}] [NONE]")

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
      ctx.output(delayOutputTag, outputRecord)

      logger.info(
        f"[DELAY] [${ctx.getCurrentKey}] [${c.topicTwo}] [${topicTwoCurrentState.get("metadata").get("timestamp").asText()}i] [${duration}] [NONE]")
    } else {
      logger.warn(f"[DELAY] [${ctx.getCurrentKey}] [NONE] [0i] [-1] [NONE]")
    }
  }
}
