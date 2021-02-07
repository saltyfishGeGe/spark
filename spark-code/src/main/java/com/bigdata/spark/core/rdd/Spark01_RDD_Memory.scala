package com.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark01_rdd")

    val sc = new SparkContext(sparkConf)

    // 内存中获取数据
    val ints = List[Int](1, 2, 3, 4)

    val rdd: RDD[Int] = sc.makeRDD(ints)

    // parallelize:
    //val rdd: RDD[Int] = sc.parallelize(ints)

    val result: Array[Int] = rdd.collect()

    result.foreach(println)

    sc.stop()
  }

}
