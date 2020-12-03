package eu.fasten.synchronization

import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class OutOfSyncWatermarkTest
    extends AnyFunSuite
    with EmbeddedKafka
    with BeforeAndAfter {

  before {
    EmbeddedKafka.start()
  }

  after {
    EmbeddedKafka.stop()
  }
}
