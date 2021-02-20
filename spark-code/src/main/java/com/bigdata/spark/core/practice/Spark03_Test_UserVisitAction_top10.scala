package com.bigdata.spark.core.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Test_UserVisitAction_top10 {

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

    // TODO 优化① 加入缓存复用
    rdd.cache()

    // 1. TOP 10 热门品类
    // 点击数
    val clickCountRDD: RDD[(String, Int)] = rdd.filter(line => {
      val columns: Array[String] = line.split("_")
      !"-1".equals(columns(6))
    })
      .map(line => {
      val columns: Array[String] = line.split("_")
      (columns(6), 1)
    })
      .reduceByKey(_ + _)

    // 下单数
    val orderCountRDD: RDD[(String, Int)] = rdd.filter(line => {
      val columns: Array[String] = line.split("_")
      !"null".equals(columns(8))
    })
      .flatMap(line => {
      val columns: Array[String] = line.split("_")
      columns(8).split(",")
    })
      .map(t => (t, 1))
      .reduceByKey(_ + _)

    // 支付数
    val payCountRDD: RDD[(String, Int)] = rdd.filter(line => {
      val columns: Array[String] = line.split("_")
      !"null".equals(columns(10))
    })
      .flatMap(line => {
      val columns: Array[String] = line.split("_")
      columns(10).split(",")
    })
      .map(t => (t, 1))
      .reduceByKey(_ + _)


    // TODO 原先cogroup算子，在分区不同时会产生shffule
    // TODO 调整数据结构(id, num) => (id, num1, 0, 0)
    val allRDD: RDD[(String, (Int, Int, Int))] = clickCountRDD.mapValues((_, 0, 0))
      .union(orderCountRDD.mapValues((0, _, 0)))
      .union(payCountRDD.mapValues((0, 0, _)))

    val result: RDD[(String, (Int, Int, Int))] = allRDD.reduceByKey((t1, t2) => {
      (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    }).sortBy(_._2, false)


    result.collect().take(10).foreach(println)

    val endTime: Long = System.currentTimeMillis()
    println("耗时：" + endTime.-(startTime))

    sc.stop()
  }

}
