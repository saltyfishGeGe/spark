package com.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_operator_coalesce {

  def main(args: Array[String]): Unit = {

    val datas = List(1,2,3,4,5,6)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    // 设置多个分区
    val rdd: RDD[Int] = sc.makeRDD(datas, 4)

    // coalesce用于缩减分区
    val value: RDD[Int] = rdd.coalesce(1)

    // 输出结果只会存在一个分区文件
    value.saveAsTextFile("output")

    sc.stop
  }

}