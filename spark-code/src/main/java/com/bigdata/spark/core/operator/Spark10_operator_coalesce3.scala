package com.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_operator_coalesce3 {

  def main(args: Array[String]): Unit = {

    val datas = List(1,2,3,4,5,6)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    // 设置2个分区
    val rdd: RDD[Int] = sc.makeRDD(datas, 2)

    // 扩大分区
    // 必须开启shuffle, 否则无法打散数据
    // 一般情况下缩减分区使用coalesce， 扩大分区可以使用repartitions，本质上调用同一算子，在API层面使用上做简易区分
    val value: RDD[Int] = rdd.coalesce(3, true)

    // rdd.coalesce(3, true) = rdd.repartition(3)
    val value2: RDD[Int] = rdd.repartition(3)

    value2.saveAsTextFile("output")

    sc.stop
  }

}