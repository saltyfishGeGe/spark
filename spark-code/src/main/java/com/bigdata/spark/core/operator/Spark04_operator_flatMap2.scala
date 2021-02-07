package com.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_operator_flatMap2 {

  def main(args: Array[String]): Unit = {


    val datas = List(List(1,2), 3, List(4,5))

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Any] = sc.makeRDD(datas, 2)

    val flatRDD = rdd.flatMap(data => {
      // 与java的switch相似，区别是match只要匹配到一个值其余都不会进行匹配
      data match {
          // List[_] : _ 代表 默认的全匹配备选项
        case list:List[_] => list
          // :Any可省略，相当于任何类型都匹配
        case dat: Any => List(dat)
      }
    })

    flatRDD.foreach(println)

    flatRDD.collect

    sc.stop
  }

}
