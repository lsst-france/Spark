package ca

import org.scalatest._

class TScalaSpec extends FlatSpec with Matchers {
  "The TScala object" should "match the argument" in {
    TScala.main("Toto") shouldEqual "Toto"
  }
}
