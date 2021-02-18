package com.bigdata.spark.core.action.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Action {

  def main (args: Array[String] ): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("action_operator")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

    // 行动算子，触发作业执行
    // 调用 sc.runJob
    // 将不通过分区中的数据按照分区顺序采集回到Driver端内存中进行
    rdd.collect()

    sc.stop()
  }

}
