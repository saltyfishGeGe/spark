package com.bigdata.spark.core.rdd.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_File {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("spark03_rdd_file")

    val sc : SparkContext = new SparkContext(sparkConf)

    // textFile: 以行为单位读取数据
    // wholeTextFiles以文件为单位读取数据，按元组读取
    // 读取结果为k,v， k为文件，v为文件内容
    val rdd : RDD[(String, String)] = sc.wholeTextFiles("datas")

//    rdd.foreach(w => {
//      println(w._1)
//      println("===")
//      println(w._2)
//    })

    // TODO 行数计算


    rdd.collect
    sc.stop
  }

}
