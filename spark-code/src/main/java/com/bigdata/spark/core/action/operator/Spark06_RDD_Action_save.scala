package com.bigdata.spark.core.action.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Action_save {

  def main (args: Array[String] ): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("action_operator")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

    // 输出为hadoop文件
    rdd.saveAsObjectFile("output1")

    // SequenceFile需要数据源为kv类型数据
    val rdd2 = sc.makeRDD(List(("a", 1),("b", 1)))
    rdd2.saveAsSequenceFile("output2")

    sc.stop()
  }

}
