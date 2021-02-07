package com.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_operator_MapPartition_Test {

  def main(args: Array[String]): Unit = {


    val datas: List[Int] = List[Int](1,2,3,4)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator_MapPartition")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(datas, 2)

    // TODO 查找出不同分区中最大的值
    val maxRDD: RDD[Int] = rdd.mapPartitions(iter => {
      List(iter.max).iterator
    })

    maxRDD.foreach(println)

    maxRDD.collect

    sc.stop
  }

}
