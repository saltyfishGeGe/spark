package com.bigdata.spark.core.transform.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_operator_aggregateByKey2 {

  def main(args: Array[String]): Unit = {

    val datas = List(("a", 1),("a", 2),("b", 3),("b", 4),("b", 5),("a", 6))

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(datas, 2)

    // 分区1 ("a", 1),("a", 2),("b", 3) => a,2  b,3
    // 分区2 ("b", 4),("b", 5),("a", 6) => b,5  a,6
    // 结果：(b,8) (a,8)
    rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y), // 分区内计算后输出值
      (x, y) => x + y // 拿上一个函数输出值作为输入值计算
    ).collect().foreach(println)

    sc.stop
  }

}