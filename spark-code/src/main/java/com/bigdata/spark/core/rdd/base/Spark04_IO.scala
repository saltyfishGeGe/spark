package com.bigdata.spark.core.rdd.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_IO {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark01_rdd")

    val sc = new SparkContext(sparkConf)


    // 生成文件
//    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",3),("d",4)))
//    rdd.saveAsTextFile("output")
//    rdd.saveAsSequenceFile("output1")
//    rdd.saveAsObjectFile("output2")

    // 读取文件
    val rdd: RDD[String] = sc.textFile("output")
    println(rdd.collect().mkString(","))

    // k-v类型
    val rdd1 = sc.sequenceFile[String, Int]("output1")
    println(rdd1.collect().mkString(","))

    // 元组类型数据
    val rdd2 = sc.objectFile[(String, Int)]("output2")
    println(rdd2.collect().mkString(","))

    sc.stop()
  }

}
