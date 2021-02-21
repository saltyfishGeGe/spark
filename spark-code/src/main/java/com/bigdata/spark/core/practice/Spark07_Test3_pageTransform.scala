package com.bigdata.spark.core.practice

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Test3_pageTransform {

  def main(args: Array[String]): Unit = {
    // 测试数据datas/user_visit_action.txt
    // 元数据：
    // 日期，用户ID, SessionID, 页面ID, 时间戳，搜索关键字,点击品类ID, 产品ID,下单品类ID，产品ID，支付品类ID，产品ID,城市ID
    // 列分隔符为_    单列中多个值使用,分隔
    // 搜索，点击，下单，支付，四种行为同时只能存在一种

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("practice01")

    val sc = new SparkContext(sparkConf)

    // TODO 3 获取每个页面的跳转转换率，即从该页面跳转去其他页面的次数/该页面访问的总次数

    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    // 数据转换
    val userVisitListRDD: RDD[UserVisitAction] = rdd.map(line => {
      val columns: Array[String] = line.split("_")
      UserVisitAction(
        columns(0),
        columns(1).toLong,
        columns(2),
        columns(3).toLong,
        columns(4),
        columns(5),
        columns(6).toLong,
        columns(7).toLong,
        columns(8),
        columns(9),
        columns(10),
        columns(11),
        columns(12).toLong
      )
    })

    userVisitListRDD.cache()

    // 假如只统计某几个页面的跳转率
    val ids = List(1, 2, 3, 4, 5)
    val okChains: List[(Int, Int)] = ids.zip(ids.tail)

    // 根据页面类型获取当前页面访问次数
    val pageVisitCount: Map[Long, Int] = userVisitListRDD
      .filter(visit => ids.init.contains(visit.page_id))
      .map(visit => (visit.page_id, 1))
      .reduceByKey(_ + _)
      .collect()
      .toMap


    // 获取页面跳转次数
    // 从1 -> 2 的次数
    // 将每个用户访问的所有页面进行分组排序，依次拉链得到用户访问页面顺序
    val mapRDD: RDD[(String, List[((Long, Long), Int)])] = userVisitListRDD.map(visit => {
      (visit.session_id, visit)
    })
      // 将用户访问的所有页面进行分组
      .groupByKey()
      // 分组内排序
      .mapValues(iter => {
      val sortList: List[UserVisitAction] = iter.toList.sortWith((t1, t2) => {
        val dfm: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
        val date1: Date = dfm.parse(t1.action_time)
        val date2: Date = dfm.parse(t2.action_time)
        if (date2.after(date1)) {
          true
        }
        false
      })

      // 进行集合内拉链
      var pageIds: List[Long] = sortList.map(_.page_id)
      var pageIdChains: List[(Long, Long)] = pageIds.zip(pageIds.tail)
      pageIdChains.filter(t => {
        okChains.contains(t)
      })
        .map(t => (t, 1))
    })

    val result: RDD[((Long, Long), Int)] = mapRDD.map(_._2).flatMap(list => list).reduceByKey(_ + _)

    result.collect().foreach {
      case ((page1, page2), sum) => {
        val all: Int = pageVisitCount.getOrElse(page1, 0)
        println(s"当前页面：${page1}, 跳转页面：${page2}, 总次数：${all}, 跳转数:${sum}, 转换率：${sum.floatValue() / all * 100}%")
      }
    }

    sc.stop()
  }

  case class UserVisitAction(
                              date: String, //用户点击行为的日期
                              user_id: Long, //用户的 ID
                              session_id: String, //Session 的 ID
                              page_id: Long, //某个页面的 ID
                              action_time: String, //动作的时间点
                              search_keyword: String, //用户搜索的关键词
                              click_category_id: Long, //某一个商品品类的 ID
                              click_product_id: Long, //某一个商品的 ID
                              order_category_ids: String, //一次订单中所有品类的 ID 集合
                              order_product_ids: String, //一次订单中所有商品的 ID 集合
                              pay_category_ids: String, //一次支付中所有品类的 ID 集合
                              pay_product_ids: String, //一次支付中所有商品的 ID 集合
                              city_id: Long
                            )

}
