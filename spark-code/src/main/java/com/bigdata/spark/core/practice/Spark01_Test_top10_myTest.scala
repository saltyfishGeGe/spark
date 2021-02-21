package com.bigdata.spark.core.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Test_top10_myTest {

  def main(args: Array[String]): Unit = {
    // 测试数据datas/user_visit_action.txt
    // 元数据：
    // 日期，用户ID, SessionID, 页面ID, 时间戳，搜索关键字,点击品类ID, 产品ID,下单品类ID，产品ID，支付品类ID，产品ID,城市ID
    // 列分隔符为_    单列中多个值使用,分隔
    // 搜索，点击，下单，支付，四种行为同时只能存在一种

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("practice01")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    // 1. TOP 10 热门品类, 通过点击数，下单数，支付数总数进行排名
    val mapRDD: RDD[(String, Int)] = rdd.map(line => {
      val columns: Array[String] = line.split("_")

      val click: String = columns(6)
      val order: String = columns(8)
      val pay: String = columns(10)

      // 点击、下单、支付只会存在一种情况
      if (!"-1".equals(click)) {
        (click, 1)
      } else if (!"null".equals(order)) {
        (order, 1)
      } else if (!"null".equals(pay)) {
        (pay, 1)
      } else {
        ("error", 0)
      }
    })

    val sortByRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_).sortBy(_._2, false)

    sortByRDD.collect().take(10).foreach(println)

    sc.stop()
  }

}
