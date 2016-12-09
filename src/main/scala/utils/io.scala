package utils

import scala.io.Source
import scala.collection.mutable.ListBuffer
object IO {
  def getSamplesFromFile(path:String):List[String]={
    var files =  new ListBuffer[String]()
    for(line <- Source.fromFile(path).getLines())
      files += line
    files.toList

  }

}