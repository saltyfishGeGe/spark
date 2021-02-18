package com.bigdata.spark.core.transform.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_operator_foldByKey {

  def main(args: Array[String]): Unit = {

    val datas = List(("a", 1),("a", 2),("b", 3),("b", 4),("b", 5),("a", 6))

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(datas, 2)

    // 当分区内逻辑和分区间逻辑相同时可用foldByKey,与aggregateByKey相同
    // (b,12)
    // (a,9)
    // foldByKey和reduceByKey的区别主要是初始值
    rdd.foldByKey(0)(_+_).collect().foreach(println)

    sc.stop
  }

}