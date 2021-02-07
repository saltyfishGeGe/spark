package com.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_operator_distinct {

  def main(args: Array[String]): Unit = {

    val datas = List(1,2,3,4,1,2,3,4)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(datas, 2)

    // map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)
    // 构造为(xxx, null)
    // reduceByKey将所有重复的值，都值输出为(xxx, null)
    // map => 取第一个值
    val disRDD: RDD[Int] = rdd.distinct()

    disRDD.foreach(println)

    disRDD.collect()

    sc.stop
  }

}