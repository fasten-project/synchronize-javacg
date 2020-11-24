package eu.fasten.synchronization

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object Main {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {
    env
      .fromCollection(List("This is just a test"))
      .flatMap(_.split(" "))
      .print()

    env.execute()
  }

}
