package com.bigdata.spark.core.transform.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_operator_filter_logTest {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.textFile("datas/apache.log")

    // 过滤2015-05-17的数据
    val filterRDD: RDD[String] = rdd.mapPartitions(iter => {
      iter.filter(line => {
        line.contains("17/05/2015")
      })
    })

    filterRDD.foreach(println)

    filterRDD.collect()

    sc.stop
  }

}
