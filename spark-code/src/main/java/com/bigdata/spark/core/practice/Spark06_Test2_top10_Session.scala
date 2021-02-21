package com.bigdata.spark.core.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayOps

object Spark06_Test2_top10_Session {

  def main(args: Array[String]): Unit = {
    // 测试数据datas/user_visit_action.txt
    // 元数据：
    // 日期，用户ID, SessionID, 页面ID, 时间戳，搜索关键字,点击品类ID, 产品ID,下单品类ID，产品ID，支付品类ID，产品ID,城市ID
    // 列分隔符为_    单列中多个值使用,分隔
    // 搜索，点击，下单，支付，四种行为同时只能存在一种

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("practice01")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    rdd.cache()

    // top10
    val top10Tuples: Array[(String, (Int, Int, Int))] = top10(rdd)

    // 拿到前十品类id
    val top10Ids: Array[String] = top10Tuples.map(_._1)

    // 2.top10热门品类中，每个品类的top10活跃Session点击的统计

    // 过滤出热门品类数据
    val fileterRDD: RDD[String] = rdd.filter(line => {
      val columns: Array[String] = line.split("_")

      if (top10Ids.contains(columns(6))) {
        true
      } else {
        false
      }
    })

    // 初步聚合数据
    val reduceRDD: RDD[((String, String), Int)] = fileterRDD.map(line => {
      val columns: Array[String] = line.split("_")
      // ((品类ID, sessionID), 1)
      ((columns(6), columns(2)), 1)
    }).reduceByKey(_ + _)

    // 转换结构，通过品类ID进行分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = reduceRDD.map(t => (t._1._1, (t._1._2, t._2))).groupByKey()

    // 分组内排序求top10
    val result: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(iter => {
      iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
    })

    // 输出结果
    result.collect().foreach(println)

    sc.stop()
  }


  // 取top10
  def top10(rdd: RDD[String]) : Array[(String, (Int, Int, Int))] = {

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

    result
  }
}
