# Synchronization Java Call Graphs
As multiple plugins process new Java versions, downstream tasks might experience synchronization issues when one of these plugins is not finished yet.
This (Flink) synchronization job solves this issue by joining multiple Kafka output topics and only emitting records when upstream tasks finished processing.
This way, downstream plugins can safely read from the output of this synchronization job to process the new Java version with the guarantee that the upstream plugins finished their jobs.

