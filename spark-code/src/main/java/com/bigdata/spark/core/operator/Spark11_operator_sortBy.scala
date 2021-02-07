package com.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_operator_sortBy {

  def main(args: Array[String]): Unit = {

    val datas = List(5,6,2,3,8,9,1)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(datas, 2)

    // 默认会进行shuffle
    // 第二个参数控制升降序，默认为true，升序
    val sortByRDD: RDD[Int] = rdd.sortBy(num => num, true)

    sortByRDD.saveAsTextFile("output")
    sc.stop
  }

}