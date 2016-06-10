package utilsTest

import org.scalatest._
import utils.IO

class utilTest extends FlatSpec with Matchers {
  "getSamples" should  "get the file list form samples" in {
    utils.IO.getSamplesFromFile("filesList.txt").size should be (3)

  }

}