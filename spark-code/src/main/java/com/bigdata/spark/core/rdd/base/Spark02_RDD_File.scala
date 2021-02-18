package com.bigdata.spark.core.rdd.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_rdd_file")

    val sc : SparkContext = new SparkContext(sparkConf)

    // 从文件中获取数据
    // 当前环境的根目录为准，可以写绝对路径，也可以写相对路径
    //val rdd: RDD[String] = sc.textFile("datas")

    // 支持通配符 *
    val rdd: RDD[String] = sc.textFile("datas/wc2*.txt")

    // 从HDFS获取文件
    // val rdd : RDD[String] = sc.textFile("hdfs://node01:8020/test1.txt")

    rdd.collect().foreach(println)

    sc.stop()
  }

}
