package com.bigdata.spark.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_operator_groupBy {

  def main(args: Array[String]): Unit = {

    val datas = List(1,2,3,4,5,6)

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(datas, 1)

    // 通过计算的key值进行分组
    val groupByRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(num => {
      num % 2
    })

    groupByRDD.foreach(println)

    groupByRDD.collect()

    sc.stop
  }

}
