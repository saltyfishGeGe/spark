package com.bigdata.spark.core.action.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Action {

  def main (args: Array[String] ): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("action_operator")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    val count: Long = rdd.count()
    println(count)

    val first: Int = rdd.first()
    println(first)

    val re: Array[Int] = rdd.take(3)
    println(re.mkString(","))

    // 排序后在取值
    val re2: Array[Int] = rdd.takeOrdered(3)(Ordering.Int.reverse)
    println(re2.mkString(","))

    sc.stop()
  }

}
