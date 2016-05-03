package steps

import org.apache.spark.Partitioner


  class BinPartitioner(bins: Int, length: Int, offset:Int) extends Partitioner {
    def numPartitions: Int = bins

    def getPartition(key: Any): Int = {


      key match {
        case null => 0
        case key: Int => math.floor( (key-offset) / length).toInt

      }
    }

    override def equals(other: Any): Boolean = {
      other match {
        case h: BinPartitioner => true
        case _ => false
      }
    }


}