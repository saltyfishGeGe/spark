package com.bigdata.spark.core.transform.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_operator_groupBy2 {

  def main(args: Array[String]): Unit = {

    val datas = List("Hello", "Spark", "Hive", "Scala", "Hadoop")

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(datas, 2)

    // 通过首字母分组
    // group By 会进行shuffle，打散重新组合
    val groupByRDD: RDD[(String, Iterable[String])] = rdd.groupBy(word => {
      word.substring(0, 1)
    })

    groupByRDD.foreach(println)

    groupByRDD.collect()

    sc.stop
  }

}
