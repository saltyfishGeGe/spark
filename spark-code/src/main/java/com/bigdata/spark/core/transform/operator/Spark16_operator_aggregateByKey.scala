package com.bigdata.spark.core.transform.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_operator_aggregateByKey {

  def main(args: Array[String]): Unit = {

    val datas = List(("a", 1),("a", 2),("a", 3),("a", 4))

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(datas, 2)

    // aggregateByKey 可独立设置分区内和分区间的逻辑
    // 存在函数柯里化，有两个函数列表
    // 第一个参数，需要传递一个初始值
    // 第二个参数：传入两个函数：①分区内计算规则 ②分区间计算规则
    rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    ).collect().foreach(println)

    sc.stop
  }

}