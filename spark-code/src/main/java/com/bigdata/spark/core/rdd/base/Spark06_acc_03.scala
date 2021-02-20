package com.bigdata.spark.core.rdd.base

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_acc_03 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark01_rdd")

    val sc = new SparkContext(sparkConf)

    // 内存中获取数据
    val ints = List[Int](1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(ints)

    // 累加器在各个executor中获取累计的副本传回driver端后，将多个结果进行merge
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")

    val mapRDD: RDD[Int] = rdd.map(num => {
      sumAcc.add(num)
      num
    })

    mapRDD.collect()

    // 累加器
    // 少加：没有行动算子不会执行
    // 多加：多个行动算子会多次累加，累加器是全局变量
    // 一般累加器会放在行动算子中进行操作
    println(sumAcc.value)

    sc.stop()
  }

}
