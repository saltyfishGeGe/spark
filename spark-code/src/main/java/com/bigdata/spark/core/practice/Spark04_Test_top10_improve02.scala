package com.bigdata.spark.core.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayOps

object Spark04_Test_top10_improve02 {

  def main(args: Array[String]): Unit = {
    // 测试数据datas/user_visit_action.txt
    // 元数据：
    // 日期，用户ID, SessionID, 页面ID, 时间戳，搜索关键字,点击品类ID, 产品ID,下单品类ID，产品ID，支付品类ID，产品ID,城市ID
    // 列分隔符为_    单列中多个值使用,分隔
    // 搜索，点击，下单，支付，四种行为同时只能存在一种

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("practice01")

    val sc = new SparkContext(sparkConf)

    val startTime: Long = System.currentTimeMillis()

    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    // TODO 去掉原有程序中大量的reduceByKey

    val result: Array[(String, (Int, Int, Int))] = rdd.flatMap(line => {
      val columns: ArrayOps.ofRef[String] = line.split("_")
      if (columns(6) != "-1") {
        // 点击，直接构造成最终数据结构
        List((columns(6), (1, 0, 0)))

      } else if (columns(8) != "null") {
        val ids: ArrayOps.ofRef[String] = columns(8).split(",")
        // 下单，需要打散该列多个值，再构造
        ids.map((_, (0, 1, 0)))

      } else if (columns(10) != "null") {
        val ids: ArrayOps.ofRef[String] = columns(8).split(",")
        // 支付，需打散该列多个值在构造
        ids.map((_, (0, 0, 1)))

      } else {
        // 不符合数据返回空
        Nil
      }
    })
      .reduceByKey((t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      })
      .sortBy(_._2, false)
      .take(10)

    result.foreach(println)

    val endTime: Long = System.currentTimeMillis()
    println("耗时：" + endTime.-(startTime))

    sc.stop()
  }

}
