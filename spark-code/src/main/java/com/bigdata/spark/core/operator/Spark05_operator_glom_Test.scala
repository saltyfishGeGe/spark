package com.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_operator_glom_Test {

  def main(args: Array[String]): Unit = {

    val datas = List(1,2,3,4,5,6,7,8)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(datas, 3)

    // TODO 分区内最大值，分区间最大值求和
    val arrRDD: RDD[Array[Int]] = rdd.glom()

    val maxRDD: RDD[Int] = arrRDD.map(arr => arr.max)

    println(maxRDD.collect().sum)
    sc.stop

//    val maxRDD: RDD[Int] = rdd.mapPartitions(iter => {
//      List(iter.max).iterator
//    })
//
//    println(maxRDD.collect().sum)
//    sc.stop()
  }

}
