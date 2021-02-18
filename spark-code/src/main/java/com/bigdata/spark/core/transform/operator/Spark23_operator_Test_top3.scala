package com.bigdata.spark.core.transform.operator

import com.bigdata.spark.core.scala_test.ProvincePartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark23_operator_Test_top3 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_operator")

    val sc : SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.textFile("datas/agent.log")

    // 时间戳、省份、城市、用户、广告
    // TODO 统计出每个省份每个广告被点击数量的top3

    val proAdvGroupRDD: RDD[((String, String), Iterable[String])] = rdd.groupBy(line => {
      val cols: Array[String] = line.split(" ")
      val province = cols(1)
      val adv = cols(3)
      (province, adv)
    })

    // ((7,72),9)
    val proAdvSizeRDD: RDD[((String, String), Int)] = proAdvGroupRDD.mapValues(iter => iter.size)

    // 改变结构(7, (72, 9))
    val mapRDD: RDD[(String, (String, Int))] = proAdvSizeRDD.map(line => {
      (line._1._1, (line._1._2, line._2))
    })

    // 分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()

    // 对每个分组中的所有value集合，转换，排序
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(iter => {
      iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    })

    resultRDD.collect().foreach(println)

    sc.stop
  }

}