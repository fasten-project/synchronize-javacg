# Synchronization Java Call Graphs
As multiple plugins process new Java versions, downstream tasks might experience synchronization issues when one of these plugins is not finished yet.
This (Flink) synchronization job solves this issue by joining multiple Kafka output topics and only emitting records when upstream tasks finished processing.
This way, downstream plugins can safely read from the output of this synchronization job to process the new Java version with the guarantee that the upstream plugins finished their jobs.

## Prerequisites
This job relies on the following (environment) variables:
- `KAFKA_BROKER`: A list of comma separated Kafka brokers, to connect to the Kafka cluster. For example: `samos:9092,delft:9092,goteborg:9092`.
- `INPUT_TOPIC_ONE`: The first input topic to read from and to join with `INPUT_TOPIC_TWO`.
- `INPUT_TOPIC_TWO`: The second input topic to read from and to join with `INPUT_TOPIC_ONE`.
- `OUTPUT_TOPIC`: The output topic to emit joined records to. This variable will be transformed to `fasten.{OUTPUT_TOPIC}.out` and `fasten.{OUTPUT_TOPIC}.err`.
- `TOPIC_ONE_KEYS`: A list of comma separated keys to join on for the _first_ topic. Supports nested JSON fields. For example: `artifact,some.nested.key`.
- `TOPIC_TWO_KEYS`: A list of comma separated keys to join on for the _second_ topic. Supports nested JSON fields. For example: `artifact,some.nested.key`.
- `WINDOW_TIME`: The time (in seconds) to keep a record in state before its removed. I.e. how long to wait before the join must be finished. 

## Data flow
<img src="sync_job.svg"/>

## Deployment