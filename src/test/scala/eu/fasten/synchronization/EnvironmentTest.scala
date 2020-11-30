package eu.fasten.synchronization

import org.scalatest.funsuite.AnyFunSuite

class EnvironmentTest extends AnyFunSuite {

  test("Empty environment should return None") {
    removeEnv("KAFKA_BROKER")
    assert(Main.verifyEnvironment() == None)
  }

  test("Simple environment test") {
    setEnv("KAFKA_BROKER", "delft:9092")
    setEnv("INPUT_TOPIC_ONE", "test1")
    setEnv("INPUT_TOPIC_TWO", "test2")
    setEnv("OUTPUT_TOPIC", "test3")
    setEnv("TOPIC_ONE_KEYS", "key1")
    setEnv("TOPIC_TWO_KEYS", "key2")
    setEnv("WINDOW_TIME", "10")

    val env = Main.verifyEnvironment()
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
    setEnv("KAFKA_BROKER", "delft:9092,samos:9092")
    setEnv("INPUT_TOPIC_ONE", "test1")
    setEnv("INPUT_TOPIC_TWO", "test2")
    setEnv("OUTPUT_TOPIC", "test3")
    setEnv("TOPIC_ONE_KEYS", "key1,key2")
    setEnv("TOPIC_TWO_KEYS", "key2")
    setEnv("WINDOW_TIME", "99")

    val env = Main.verifyEnvironment()
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

  def setEnv(key: String, value: String) = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    val map = field
      .get(System.getenv())
      .asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]
    map.put(key, value)
  }

  def removeEnv(key: String): Unit = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    val map = field
      .get(System.getenv())
      .asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]

    if (map.containsKey(key))
      map.remove(key)
  }

}
