package eu.fasten.synchronization

import org.scalatest.funsuite.AnyFunSuite

class EnvironmentTest extends AnyFunSuite {

  test("Empty environment should return None") {
    assert(Main.verifyConfig(Array("")) == None)
  }

  test("Simple environment test") {
    val arg =
      "-b delft:9092 --topic_one test1 --topic_two test2 -o test3 --topic_one_keys key1 --topic_two_keys key2 -w 10 --delay_topic delay --enable_delay true"

    val env = Main.verifyConfig(arg.split(" "))
    assert(env.isDefined)
    assert(env.get.brokers(0) == "delft:9092")
    assert(env.get.topicOne == "test1")
    assert(env.get.topicTwo == "test2")
    assert(env.get.outputTopic == "test3")
    assert(env.get.topicOneKeys(0) == "key1")
    assert(env.get.topicTwoKeys(0) == "key2")
    assert(env.get.windowTime == 10)
  }

  test("More complex environment test") {
    val arg =
      "-b delft:9092,samos:9092 --topic_one test1 --topic_two test2 -o test3 --topic_one_keys key1,key2 --topic_two_keys key2 -w 99 --delay_topic delay --enable_delay true"

    val env = Main.verifyConfig(arg.split(" "))
    assert(env.isDefined)
    assert(env.get.brokers(0) == "delft:9092")
    assert(env.get.brokers(1) == "samos:9092")
    assert(env.get.brokers.length == 2)
    assert(env.get.topicOne == "test1")
    assert(env.get.topicTwo == "test2")
    assert(env.get.outputTopic == "test3")
    assert(env.get.topicOneKeys(0) == "key1")
    assert(env.get.topicOneKeys(1) == "key2")
    assert(env.get.topicTwoKeys(0) == "key2")
    assert(env.get.topicOneKeys.length == 2)
    assert(env.get.windowTime == 99)
  }

}
