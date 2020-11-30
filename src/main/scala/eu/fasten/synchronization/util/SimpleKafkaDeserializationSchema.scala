package eu.fasten.synchronization.util

import org.apache.flink.runtime.JobException
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

class SimpleKafkaDeserializationSchema(includeMetadata: Boolean,
                                       maxRecords: Int = -1)
    extends JSONKeyValueDeserializationSchema(includeMetadata) {

  var counter: Int = 0

  override def isEndOfStream(nextElement: ObjectNode): Boolean = {
    if (maxRecords == -1)
      return false

    if (counter >= maxRecords)
      throw new JobException(
        s"Stop execution after receiving more than ${counter} records.")
    else
      false
  }

  override def deserialize(
      record: ConsumerRecord[Array[Byte], Array[Byte]]): ObjectNode = {
    if (maxRecords != -1)
      counter += 1
    super.deserialize(record)
  }
}
