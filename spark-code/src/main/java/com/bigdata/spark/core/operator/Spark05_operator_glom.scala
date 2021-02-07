package com.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_operator_glom {

  def main(args: Array[String]): Unit = {

    val datas = List(1,2,3,4,5)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(datas, 2)

    // Int => Array, 与flatMap刚好相反
    // 分区后，将分区内的数据转换为数组，结果将输出两个数组
    val glomRDD: RDD[Array[Int]] = rdd.glom()

//    glomRDD.foreach(arr => {
//      arr.foreach(num => {
//        print(num + ",")
//      })
//      println()
//    })
//
//    glomRDD.collect()

    glomRDD.collect().foreach(data => println(data.mkString(",")))

    sc.stop
  }

}
