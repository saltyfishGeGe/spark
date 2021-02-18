package com.bigdata.spark.core.action.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Action_reduce {

  def main (args: Array[String] ): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("action_operator")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    // 转换算子不会触发作业执行，只会返回新的rdd
    val re: Int = rdd.reduce((k1, k2) => { k1 + k2})

    println(re)

    sc.stop()
  }

}
