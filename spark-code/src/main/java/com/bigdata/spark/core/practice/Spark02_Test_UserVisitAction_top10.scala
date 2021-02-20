package com.bigdata.spark.core.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Test_UserVisitAction_top10 {

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


    // 将结果串联起来
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountRDD.cogroup(orderCountRDD, payCountRDD)

    val result: Array[(String, (Int, Int, Int))] = cogroupRDD.mapValues(t => {
      var clickCnt = 0
      var orderCnt = 0
      var payCnt = 0

      val clickIter: Iterator[Int] = t._1.iterator
      if (clickIter.hasNext) {
        clickCnt = clickIter.next()
      }

      val orderIter: Iterator[Int] = t._2.iterator
      if (orderIter.hasNext) {
        orderCnt = orderIter.next()
      }

      val payIter: Iterator[Int] = t._3.iterator
      if (payIter.hasNext) {
        payCnt = payIter.next()
      }

      (clickCnt, orderCnt, payCnt)
    }).sortBy(_._2, false).take(10)

    result.foreach(println)

    val endTime: Long = System.currentTimeMillis()
    println("耗时：" + endTime.-(startTime))

    sc.stop()
  }

}
