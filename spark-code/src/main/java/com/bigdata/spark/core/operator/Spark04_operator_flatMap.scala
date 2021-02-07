package com.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_operator_flatMap {

  def main(args: Array[String]): Unit = {


    val datas : List[List[Int]] = List(List(1,2), List(3,4))

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[List[Int]] = sc.makeRDD(datas, 2)

    // 传入集合
    // 返回一个可迭代的集合，类型与传入集合的类型相同
    // 此处flatMap传入的数据元为：List[Int], 则返回值也许构造成List[Int]作为函数返回值
    // 思路可参考wordCount针对于String类型的扁平化
    val flatRDD: RDD[Int] = rdd.flatMap(list => {
      list
    })

    flatRDD.foreach(println)

    flatRDD.collect

    sc.stop
  }

}
