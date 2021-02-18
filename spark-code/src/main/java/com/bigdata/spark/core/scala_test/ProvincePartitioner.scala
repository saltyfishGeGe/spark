package com.bigdata.spark.core.scala_test

import org.apache.spark.Partitioner

class ProvincePartitioner(partitions: Int) extends Partitioner {

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = key match {
    case t:Tuple2[String, String] => t._1.toInt
    case dat => 10
  }
}
