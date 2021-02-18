package com.bigdata.spark.core.action.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Action_foreach {

  def main (args: Array[String] ): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("action_operator")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

    // result
    //1
    //2
    //3
    //4
    //=========================
    //3
    //4
    //1
    //2

    // 这儿的foreach是在driver端内存中循环遍历
    // 即 先collect完数据后，在遍历，此foreach为数组方法
    rdd.collect().foreach(println)
    println("=========================")
    // 在executor端的内存中打印
    // foreach为行动算子，在executor端触发作业操作，此foreach为rdd方法
    rdd.foreach(println)


    sc.stop()
  }

}
