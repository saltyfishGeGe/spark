package com.bigdata.spark.core.rdd.base

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_acc_02 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark01_rdd")

    val sc = new SparkContext(sparkConf)

    // 内存中获取数据
    val ints = List[Int](1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(ints)

    // 累加器在各个executor中获取累计的副本传回driver端后，将多个结果进行merge
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")

    rdd.foreach(num => sumAcc.add(num))

    println(sumAcc.value)

    sc.stop()
  }

}
