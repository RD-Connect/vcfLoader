package steps

import org.apache.spark.Partitioner


  class BinPartitioner(bins: Int, length: Int, offset:Int) extends Partitioner {
    def numPartitions: Int = bins

    def getPartition(key: Any): Int = {


      key match {
        case null => 0
        case key: Int => { val partition=math.floor( (key-offset) / length).toInt
        if (partition>numPartitions-1) numPartitions-1
        else partition}

      }
    }

    override def equals(other: Any): Boolean = {
      other match {
        case h: BinPartitioner => true
        case _ => false
      }
    }


}