package com.bigdata.spark.core.transform.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_operator_sortBy_Test {

  def main(args: Array[String]): Unit = {

    val datas = List(Tuple2("1","yang1"), Tuple2("5","yang5"), Tuple2("2","yang2"))

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, String)] = sc.makeRDD(datas, 1)

    // 默认会进行shuffle
    val sortByRDD: RDD[(String, String)] = rdd.sortBy(tu => tu._1, false)

    sortByRDD.saveAsTextFile("output")

    sc.stop
  }

}