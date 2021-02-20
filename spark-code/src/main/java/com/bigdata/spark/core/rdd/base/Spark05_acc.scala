package com.bigdata.spark.core.rdd.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_acc {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark01_rdd")

    val sc = new SparkContext(sparkConf)

    // 内存中获取数据
    val ints = List[Int](1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(ints)

    var sum = 0;

    rdd.foreach(num => sum += num)

    // 分布式环境下，简单的foreach无法做累加。在各自executor中累加完成后无法返回driver端，所以最后输出0
    println(sum)

    sc.stop()
  }

}
