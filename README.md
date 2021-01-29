# Synchronization Kafka Topics ![Scala CI](https://github.com/fasten-project/synchronize-javacg/workflows/Scala%20CI/badge.svg)
As multiple plugins process new recods in paralllel, downstream tasks might experience synchronization issues when one of these plugins isn't finished yet.
This (Flink) synchronization job solves this issue by joining multiple Kafka output topics and only emitting records when upstream tasks finished processing.
This way, downstream plugins can safely read from the output of this synchronization job to process the new records with the guarantee that the upstream plugins finished their jobs.

## Usage
```bash
Flink Synchronization Job
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
                           The time before a delay message is sent when a record is unjoined.
  --delay_topic <string>   The topic to output delayed messages to.
  --enable_delay <boolean>
                           Enable or disable delayed messages.
  -p, --production         Adding this flag will run the Flink job in production (enabling checkpointing, restart strategies etc.)
  --parallelism <value>    The amount of parallel workers for Flink.
  --backendFolder <value>  Folder to store checkpoint data of Flink.
```
## Output
Output to the `topic_prefix.output_topic.out` topic is:
```json
{
  "key": "example:coord:1.0",
  "topic_one": { ... },
  "topic_two": { ... }
}
```

Output to the `topic_prefix.delay_topic.out` topic is:
```json
{
  "key": "example:coord:1.0",
  "error": "Missing information from topic_one.",
  "topic_two": { ... }
}
```

If multiple keys are used for joining, the individual keys are concatenated using `:`. `topic_one` and `topic_two` will be replaced with the actual topic names and the contents will be equal to the output of these topics. 


## Local deployment
Ensure that you can pull the Github Packages container in your local environment. See [this page](https://docs.github.com/en/packages/guides/configuring-docker-for-use-with-github-packages#authenticating-to-github-packages). 

Pull the Docker container.
```
docker pull docker.pkg.github.com/fasten-project/synchronize-javacg/syncjob:latest
```

Create a network.
```
docker network create flink-network
```

Run a JobManager to deploy the job.
```bash
docker run \
     --env FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager" \
     --name=jobmanager \
     --network flink-network \
     docker.pkg.github.com/fasten-project/synchronize-javacg/syncjob:latest standalone-job \
     --job-classname eu.fasten.synchronization.Main \
     [job arguments]
```
For the `[job arguments]`, see the parameters above.

Then run a TaskManager to run the job.
```bash
docker run \
      --env FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager" \
      --network flink-network \
      docker.pkg.github.com/fasten-project/synchronize-javacg/syncjob:latest \
      taskmanager
```

An example JobManager deployment _including_ (dummy) job arguments:
```bash
docker run \
     --env FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager" \
     --name=jobmanager \
     --network flink-network \
     docker.pkg.github.com/fasten-project/synchronize-javacg/syncjob:latest standalone-job \
     --job-classname eu.fasten.synchronization.Main \
     -b localhost:9092 \
     --topic_one "topic_one" \
     --topic_two "topic_two" \
     -o "output_topic"  \
     --topic_one_keys "input.input.key1,input.input,key2" \
     --topic_two_keys "key1,key2" \
     -w 180 \
     --enable_delay false \
     --delay_topic "delay"
```
