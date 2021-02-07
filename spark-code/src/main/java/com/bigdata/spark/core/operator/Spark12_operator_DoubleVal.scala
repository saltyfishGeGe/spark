package com.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_operator_DoubleVal {

  def main(args: Array[String]): Unit = {

    val datas1 = List(1,2,3,4,5)

    val datas2 = List(3,4,5,6,7)

    val datas3 = List("31", "41", "51", "61", "71")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd1: RDD[Int] = sc.makeRDD(datas1, 1)

    val rdd2: RDD[Int] = sc.makeRDD(datas2, 1)

    val rdd3: RDD[String] = sc.makeRDD(datas3, 1 )

    /**
      * 集合操作，要求两个rdd数据类型相同
      * 拉链则无数据类型要求
      */

    // 交集
    val jiaoji: RDD[Int] = rdd1.intersection(rdd2)

    // 并集
    val bingji: RDD[Int] = rdd1.union(rdd2)

    // 差集
    val chaji: RDD[Int] = rdd1.subtract(rdd2)

    // 拉链，将相同位置的数据拉一起
    val lalian: RDD[(Int, String)] = rdd1.zip(rdd3)

    println("交集=====")
    println(jiaoji.collect().mkString(","))
    println("并集=====")
    println(bingji.collect().mkString(","))
    println("差集=====")
    println(chaji.collect().mkString(","))
    println("拉链=====")
    println(lalian.collect().mkString(","))

    sc.stop
  }

}