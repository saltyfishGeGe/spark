package com.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_operator_MapPartitionIndex {

  def main(args: Array[String]): Unit = {


    val datas: List[Int] = List[Int](1,2,3,4)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator_MapPartition")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(datas, 2)

    val mapRDD: RDD[Int] = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else {
          Nil.iterator
        }
      })

    mapRDD.foreach(println)

    mapRDD.collect

    sc.stop
  }

}
