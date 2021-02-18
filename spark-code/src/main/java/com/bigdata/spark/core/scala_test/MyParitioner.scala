package com.bigdata.spark.core.scala_test

import org.apache.spark.Partitioner

/**
  * 自定义分区器
  * @param partitions
  */
class MyParitioner(partitions: Int) extends Partitioner{

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = key match {
    case t: Int => {
      t % 2
    }
    case dat: Any => 2
  }
}
