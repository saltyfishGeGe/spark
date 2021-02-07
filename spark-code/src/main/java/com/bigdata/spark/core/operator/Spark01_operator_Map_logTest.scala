package com.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_operator_Map_logTest {

  // TODO 读取apache.log中的url信息
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator_Map")

    val sc : SparkContext = new SparkContext(sparkConf)

    val log: RDD[String] = sc.textFile("datas/apache.log")

    val urlRDD: RDD[String] = log.map(line => {
      val words: Array[String] = line.split(" ")
      words(6)
    })


    urlRDD.foreach(println)

    urlRDD.collect()

    sc.stop()
  }

}
