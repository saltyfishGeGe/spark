package com.bigdata.spark.core.transform.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_operator_coalesce2 {

  def main(args: Array[String]): Unit = {

    val datas = List(1,2,3,4,5,6)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    // 设置3个分区
    val rdd: RDD[Int] = sc.makeRDD(datas, 3)

    // coalesce用于缩减分区
    // 合并完后，默认情况下，coalesce不会进行shuffle打伞数据，分区内容并非平均分配
    // 可能出现数据倾斜
    // val value: RDD[Int] = rdd.coalesce(2)

    // 第二个参数开启shuffle。所有分区数据均打散
    val value: RDD[Int] = rdd.coalesce(2, true)

    value.saveAsTextFile("output")

    sc.stop
  }

}