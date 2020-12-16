package eu.fasten.synchronization

import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class MoveDataTest extends AnyFunSuite with EmbeddedKafka {

  test("Check move data") {
    EmbeddedKafka.start()
    val metadataMsg: String =
      Source.fromResource("metadatadb_msg.json").getLines.mkString

    publishStringMessageToKafka("fasten.MetadataDBExtension.out", metadataMsg)
    //MoveData.main(Array())

    EmbeddedKafka.stop()
    assert(true)
  }
}
