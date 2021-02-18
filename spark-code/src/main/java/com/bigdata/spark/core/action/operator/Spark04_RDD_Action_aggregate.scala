package com.bigdata.spark.core.action.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Action_aggregate {

  def main (args: Array[String] ): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("action_operator")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

    // aggregateByKey: 初始值只会参加分区内计算
    // aggregate: 初始值会参与分区内计算和分区间计算
    // result: 40
    println(rdd.aggregate(10)(
      _ + _,
      _ + _
    ))

    // 分区内和分区间逻辑相同时
    println(rdd.fold(10)(_+_))

    sc.stop()
  }

}
