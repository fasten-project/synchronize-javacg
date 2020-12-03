# Synchronization Java Call Graphs
As multiple plugins process new Java versions, downstream tasks might experience synchronization issues when one of these plugins is not finished yet.
This (Flink) synchronization job solves this issue by joining multiple Kafka output topics and only emitting records when upstream tasks finished processing.
This way, downstream plugins can safely read from the output of this synchronization job to process the new Java version with the guarantee that the upstream plugins finished their jobs.

## Usage
```bash
Usage: Main [options]

  -b, --brokers <broker1>,<broker2>,...
                           A set of Kafka brokers to connect to.
  --topic_one <topic>      The first Kafka topic to connect to.
  --topic_two <topic>      The second Kafka topic to connect to.
  -o, --output_topic <topic>
                           The output Kafka topic, --topic_prefix will be prepended.
  --topic_prefix <value>   Prefix to add for the output topic. E.g. "fasten". 
  --topic_one_keys <key1>,<key2>,...
                           A set of keys used for the first topic. To get nested keys use ".". 
  --topic_two_keys <key1>,<key2>,...
                           A set of keys used for the first topic. To get nested keys use ".". 
  -w, --window_time <seconds>
                           The time to keep unjoined records in state. In seconds.
  -p, --production         Adding this flag will run the Flink job in production (enabling checkpointing, restart strategies etc.)
```
## Data flow
<img src="sync_job.svg"/>

## Output
Output to the `topic_prefix.output_topic.out` topic is:
```json
{
  "key": "example:coord:1.0",
  "topic_one": { ... },
  "topic_two": { ... }
}
```

Output to the `topic_prefix.output_topic.err` topic is:
```json
{
  "key": "example:coord:1.0",
  "error: "Missing information from topic_one.",
  "topic_two": { ... }
}
```

If multiple keys are used for joining, the individual keys are concatenated using `:`.
