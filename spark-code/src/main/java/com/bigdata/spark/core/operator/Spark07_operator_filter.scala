package com.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_operator_filter {

  def main(args: Array[String]): Unit = {

    val datas = List(1,2,3,4,5,6)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(datas, 2)

    // 返回false为过滤
    // 过滤奇数
    // 筛选后数据分区不变，但可能导致数据不均出现数据倾斜
    val filterRDD: RDD[Int] = rdd.filter(num => num % 2 == 0)

    filterRDD.foreach(println)

    sc.stop
  }

}
