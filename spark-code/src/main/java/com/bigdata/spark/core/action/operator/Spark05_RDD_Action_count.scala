package com.bigdata.spark.core.action.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Action_count {

  def main (args: Array[String] ): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("action_operator")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,1,1,2,2,3), 2)

    val countByValue: collection.Map[Int, Long] = rdd.countByValue()

    // Map(4 -> 1, 2 -> 3, 1 -> 3, 3 -> 2)
    // 计算值中出现的次数
    println("countByValue: " + countByValue)


    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",1),("c",1),("c",2)))

    // 统计出现该key值的次数
    println("countByKey: " + rdd2.countByKey())

    sc.stop()
  }

}
